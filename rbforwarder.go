package rbforwarder

import (
	"bytes"
	"time"

	"github.com/Sirupsen/logrus"
)

var logger *logrus.Entry

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
	backend *backend
	reports chan Report
	close   chan struct{}
	counter uint64

	config Config
}

// NewRBForwarder creates a new Forwarder object
func NewRBForwarder(config Config) *RBForwarder {
	logger = NewLogger("forwarder")

	forwarder := &RBForwarder{
		backend: &backend{
			decoderPool:   make(chan chan *Message, config.Workers),
			processorPool: make(chan chan *Message, config.Workers),
			encoderPool:   make(chan chan *Message, config.Workers),
			senderPool:    make(chan chan *Message, config.Workers),

			messages:    make(chan *Message, config.QueueSize),
			reports:     make(chan *Message, config.QueueSize),
			messagePool: make(chan *Message, config.QueueSize),

			workers: config.Workers,
			retries: config.Retries,
		},
		config:  config,
		reports: make(chan Report, config.QueueSize),
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

// Start spawn the workers
func (f *RBForwarder) Start() {
	// Start listening
	if f.backend.source == nil {
		logger.Fatal("No source defined")
	}
	f.backend.source.Listen(f)
	logger.Info("Source ready")

	for i := 0; i < f.backend.workers; i++ {
		f.backend.startDecoder(i)
		f.backend.startProcessor(i)
		f.backend.startEncoder(i)
		f.backend.startSender(i)
	}

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

// TakeMessage returns a message from the message pool
func (f *RBForwarder) TakeMessage() (message *Message, err error) {
	select {
	case message = <-f.backend.messagePool:
		return
	// case <-time.After(1 * time.Second):
	default:
		logger.Warn("Error taking message from pool")
		time.Sleep(500 * time.Millisecond)
	}

	return
}

// GetReports is used by the source to get a report for a sent message.
// Reports are delivered on the same order that was sent
func (f *RBForwarder) GetReports() <-chan Report {

	go func() {

		// Get reports from the channel
		for message := range GetOrderedMessages(f.backend.reports) {

			if message.report.StatusCode == 0 {

				// Success
				f.counter++

				message.InputBuffer.Reset()
				message.OutputBuffer.Reset()
				message.Data = nil
				message.Metadata = make(map[string]interface{})

				// Send back the message to the pool
				select {
				case f.backend.messagePool <- message:
				case <-time.After(1 * time.Second):
					logger.Error("Can't put back the message on the pool")
				}

				// Send to the source a copy of the report
				report := message.report
				message.report = Report{}

				select {
				case f.reports <- report:
				case <-time.After(1 * time.Second):
					logger.Error("Error on report: Full queue")
				}
			} else {

				// Fail
				if f.backend.retries < 0 || message.report.Retries < f.backend.retries {
					// Retry this message
					message.report.Retries++
					logger.Warnf("Retrying message: %d | Reason: %s",
						message.report.ID, message.report.Status)
					if err := message.Produce(); err != nil {
						logger.Error(err)
					}
				} else {

					// Give up
					message.InputBuffer.Reset()
					message.OutputBuffer.Reset()
					message.Data = nil
					message.Metadata = make(map[string]interface{})

					// Send back the message to the pool
					select {
					case f.backend.messagePool <- message:
					case <-time.After(1 * time.Second):
						logger.Error("Can't put back the message on the pool")
					}

					// Send to the source a copy of the report
					report := message.report
					message.report = Report{}

					select {
					case f.reports <- report:
					case <-time.After(1 * time.Second):
						logger.Error("Error on report: Full queue")
					}
				}
			}
		}
	}()

	return f.reports
}
