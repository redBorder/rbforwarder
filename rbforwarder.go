package rbforwarder

import (
	"github.com/Sirupsen/logrus"
	"github.com/redBorder/rbforwarder/pipeline"
)

// Version is the current tag
var Version = "0.4-beta3"

var log = logrus.New()

// Logger for the package
var Logger = logrus.NewEntry(log)

// RBForwarder is the main objecto of the package. It has the main methods for
// send messages and get reports. It has a backend for routing messages between
// workers
type RBForwarder struct {
	backend           *Backend
	messageHandler    *messageHandler
	currentProducedID uint64

	config Config
}

// NewRBForwarder creates a new Forwarder object
func NewRBForwarder(config Config) *RBForwarder {
	forwarder := &RBForwarder{
		backend: NewBackend(config.Workers, config.QueueSize, config.MaxMessages, config.MaxBytes),
		config:  config,
	}

	forwarder.messageHandler = newMessageHandler(
		config.Retries,
		config.Backoff,
		config.QueueSize,
		forwarder.backend.input,
	)

	fields := logrus.Fields{
		"workers":      config.Workers,
		"retries":      config.Retries,
		"backoff_time": config.Backoff,
		"queue_size":   config.QueueSize,
		"max_messages": config.MaxMessages,
		"max_bytes":    config.MaxBytes,
	}

	Logger.WithFields(fields).Debug("Initialized rB Forwarder")

	return forwarder
}

// Close stop pending actions
func (f *RBForwarder) Close() {
	f.messageHandler.close <- struct{}{}
}

// SetSender set a sender on the backend
func (f *RBForwarder) SetSender(sender pipeline.Sender) {
	f.backend.sender = sender

	// Start the backend
	f.backend.Init()

	// Start the report handler
	f.messageHandler.Init()
}

// GetReports is used by the source to get a report for a sent message.
// Reports are delivered on the same order that was sent
func (f *RBForwarder) GetReports() <-chan Report {
	return f.messageHandler.GetReports()
}

// GetOrderedReports is the same as GetReports() but the reports are delivered
// in order
func (f *RBForwarder) GetOrderedReports() <-chan Report {
	return f.messageHandler.GetOrderedReports()
}

// Produce is used by the source to send messages to the backend
func (f *RBForwarder) Produce(buf []byte, options map[string]interface{}) error {
	seq := f.currentProducedID
	f.currentProducedID++

	message := &message{
		seq:     seq,
		opts:    options,
		channel: f.messageHandler.input,
	}

	message.PushData(buf)
	f.backend.input <- message

	return nil
}
