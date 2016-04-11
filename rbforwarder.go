package rbforwarder

import (
	"bytes"
	"errors"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
)

// Logger for the package
var logger *logrus.Entry

//------------------------------------------------------------------------------
// RBForwarder
//------------------------------------------------------------------------------

// Config stores the configuration for a forwarder
type Config struct {
	Retries     int
	Backoff     int
	Workers     int
	QueueSize   int
	MaxMessages uint64
	MaxBytes    uint64
	ShowCounter int
	Debug       bool
}

// RBForwarder is the main objecto of the package. It has the main methods for
// send messages and get reports. It has a backend for routing messages between
// workers
type RBForwarder struct {
	backend       *backend
	reportHandler *reportHandler
	reports       chan Report
	counter       uint64

	keepSending     chan struct{}
	currentMessages uint64
	currentBytes    uint64

	config Config
}

// NewRBForwarder creates a new Forwarder object
func NewRBForwarder(config Config) *RBForwarder {
	if config.Debug {
		LogLevel(logrus.DebugLevel)
	}

	logger = NewLogger("backend")

	forwarder := &RBForwarder{
		backend:       newBackend(config.Workers, config.QueueSize),
		reportHandler: newReportHandler(config.Retries, config.Backoff, config.QueueSize),
		reports:       make(chan Report, config.QueueSize),
		keepSending:   make(chan struct{}),
		config:        config,
	}

	for i := 0; i < config.QueueSize; i++ {
		forwarder.backend.messagePool <- &Message{
			Metadata:     make(map[string]interface{}),
			InputBuffer:  new(bytes.Buffer),
			OutputBuffer: new(bytes.Buffer),

			backend: forwarder.backend,
		}
	}

	fields := logrus.Fields{
		"workers":    config.Workers,
		"retries":    config.Retries,
		"queue_size": config.QueueSize,
	}
	logger.WithFields(fields).Info("Initialized rB Forwarder")

	return forwarder
}

// Start spawning workers
func (f *RBForwarder) Start() {

	// Start the backend
	f.backend.Init(f.config.Workers)
	logger.Info("Backend ready")

	// Start the report handler
	f.reportHandler.Init()
	logger.Info("Reporter ready")

	if f.config.ShowCounter > 0 {
		go func() {
			for {
				timer := time.NewTimer(
					time.Duration(f.config.ShowCounter) * time.Second,
				)
				<-timer.C
				if f.counter > 0 {
					logger.Infof(
						"Messages per second %d",
						f.counter/uint64(f.config.ShowCounter),
					)
					f.counter = 0
				}
			}
		}()
	}

	// Limit the messages/bytes per second
	go func() {
		for {
			timer := time.NewTimer(1 * time.Second)
			<-timer.C
			f.keepSending <- struct{}{}
		}
	}()

	// Get reports from the backend and send them to the reportHandler
	go func() {
		for message := range f.backend.reports {
			if message.report.StatusCode == 0 {
				atomic.AddUint64(&f.counter, 1)
				atomic.AddUint64(&f.currentMessages, 1)
				atomic.AddUint64(&f.currentBytes, uint64(message.OutputBuffer.Len()))
			}
			f.reportHandler.in <- message
		}
	}()

	// Listen for reutilizable messages and send them back to the pool
	go func() {
		defer func() {
			recover()
		}()
		for message := range f.reportHandler.freedMessages {
			f.backend.messagePool <- message
		}
	}()
}

// Close stop pending actions
func (f *RBForwarder) Close() {
	close(f.reportHandler.freedMessages)
	close(f.backend.messagePool)
	f.reportHandler.close <- struct{}{}
}

// SetSenderHelper set a sender on the backend
func (f *RBForwarder) SetSenderHelper(SenderHelper SenderHelper) {
	f.backend.senderHelper = SenderHelper
}

// GetReports is used by the source to get a report for a sent message.
// Reports are delivered on the same order that was sent
func (f *RBForwarder) GetReports() <-chan Report {
	return f.reportHandler.GetReports()
}

// GetOrderedReports is the same as GetReports() but the reports are delivered
// in order
func (f *RBForwarder) GetOrderedReports() <-chan Report {
	return f.reportHandler.GetOrderedReports()
}

// TakeMessage returns a message from the message pool
func (f *RBForwarder) TakeMessage() (message *Message, err error) {

	// Wait if the limit has ben reached
	if f.config.MaxMessages > 0 && f.currentMessages >= f.config.MaxMessages {
		<-f.keepSending
		atomic.StoreUint64(&f.currentMessages, 0)
	} else if f.config.MaxBytes > 0 && f.currentBytes >= f.config.MaxBytes {
		<-f.keepSending
		atomic.StoreUint64(&f.currentBytes, 0)
	}

	message, ok := <-f.backend.messagePool
	if !ok {
		err = errors.New("Pool closed")
	}
	return
}
