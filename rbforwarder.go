package rbforwarder

import (
	"errors"
	"sync/atomic"

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
	backend        *Backend
	messageHandler *messageHandler

	currentProducedID uint64
	working           uint32

	pipeline chan *message // Messages to the backend
	handler  chan *message // Messages to message handler
}

// NewRBForwarder creates a new Forwarder object
func NewRBForwarder(config Config) *RBForwarder {
	pipeline := make(chan *message, config.QueueSize)
	handler := make(chan *message, config.QueueSize)

	forwarder := &RBForwarder{
		working: 1,
		backend: NewBackend(pipeline, handler),
		messageHandler: newMessageHandler(
			config.Retries,
			config.Backoff,
			handler,
			pipeline,
		),
		pipeline: pipeline,
		handler:  handler,
	}

	fields := logrus.Fields{
		"retries":      config.Retries,
		"backoff_time": config.Backoff,
		"queue_size":   config.QueueSize,
	}

	Logger.WithFields(fields).Debug("Initialized rB Forwarder")

	return forwarder
}

// Close stop pending actions
func (f *RBForwarder) Close() {
	atomic.StoreUint32(&f.working, 0)
	close(f.backend.input)
}

// PushComponents adds a new component to the pipeline
func (f *RBForwarder) PushComponents(components []pipeline.Composer, w []int) {
	for i, component := range components {
		f.backend.PushComponent(component, w[i])
	}
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
	if atomic.LoadUint32(&f.working) == 0 {
		return errors.New("Forwarder has been closed")
	}

	seq := f.currentProducedID
	f.currentProducedID++

	message := &message{
		seq:  seq,
		opts: options,
	}

	message.PushData(buf)
	f.pipeline <- message

	return nil
}
