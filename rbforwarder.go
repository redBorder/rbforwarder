package rbforwarder

import (
	"bytes"
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
	Workers     int
	QueueSize   int
	ShowCounter int
}

// RBForwarder is the main objecto of the package. It has the main methods for
// send messages and get reports. It has a backend for routing messages between
// workers
type RBForwarder struct {
	backend       *backend
	reportHandler *reportHandler
	reports       chan Report
	close         chan struct{}
	counter       uint64

	config Config
}

// NewRBForwarder creates a new Forwarder object
func NewRBForwarder(config Config) *RBForwarder {
	logger = NewLogger("backend")

	forwarder := &RBForwarder{
		backend:       newBackend(config.Workers, config.QueueSize),
		reportHandler: newReportHandler(config.Retries),
		reports:       make(chan Report, config.QueueSize),
		config:        config,
	}

	forwarder.close = make(chan struct{})

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

	// Start listening
	if f.backend.source == nil {
		logger.Fatal("No source defined")
	}

	// Start the backend
	f.backend.Init(f.config.Workers)
	logger.Info("Backend ready")

	// Start the report handler
	f.reportHandler.Init()
	logger.Info("Reporter ready")

	// Start the listener
	f.backend.source.Listen(f)
	logger.Info("Source ready")

	if f.config.ShowCounter > 0 {
		go func() {
			for {
				timer := time.NewTimer(
					time.Duration(f.config.ShowCounter) * time.Second,
				)
				<-timer.C
				logger.Infof(
					"Messages per second %d",
					f.counter/uint64(f.config.ShowCounter),
				)
				f.counter = 0
			}
		}()
	}

	// Get reports from the backend and send them to the reportHandler
	go func() {
		for message := range f.backend.reports {
			f.reportHandler.in <- message
		}
	}()

	// Listen for reutilizable messages and send them back to the pool
	go func() {
		for message := range f.reportHandler.freedMessages {
			f.backend.messagePool <- message
		}
	}()

	<-f.close
	f.backend.source.Close()
}

// Close stops the workers
func (f *RBForwarder) Close() {
	f.close <- struct{}{}
}

// SetSource set a source on the backend
func (f *RBForwarder) SetSource(source Source) {
	f.backend.source = source
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
	for {
		select {
		case message = <-f.backend.messagePool:
			return
		case <-time.After(500 * time.Millisecond):
			logger.Warn("Error taking message from pool")
		}
	}
}
