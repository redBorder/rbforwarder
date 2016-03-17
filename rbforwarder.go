package rbforwarder

import (
	"bytes"
	"time"

	"github.com/Sirupsen/logrus"
)

var logger *logrus.Entry

// Source is the component that gets data from a source, then sends the data
// to the backend.
type Source interface {
	Listen(Forwarder)
	Close()
}

// Decoder is the component that parses a raw buffer to a structure
type Decoder interface {
	Init(int) error
	Decode(*Message) error
}

// Processor performs operations on a data structure
type Processor interface {
	Init(int) error
	Process(message *Message) (bool, error)
}

// Encoder serializes a data structure to a output buffer
type Encoder interface {
	Init(int) error
	Encode(*Message) error
}

// Sender takes a raw buffer and sent it using a network protocol
type Sender interface {
	Init(int) error
	Send(*Message) error
}

// SenderHelper is used to create Senders instances
type SenderHelper interface {
	CreateSender() Sender
}

// Forwarder is the interface to implement by RBForwarder
type Forwarder interface {
	TakeMessage() (message *Message, err error)
	GetReports() <-chan Report
}

// RBForwarder is the main objecto of the package. It has the main methods for
// send messages and get reports. It has a backend for routing messages between
// workers
type RBForwarder struct {
	backend *backend
	reports chan Report

	retries int
	workers int

	close chan struct{}
}

// NewRBForwarder creates a new Forwarder object
func NewRBForwarder(workers, queueSize int) *RBForwarder {
	logger = NewLogger("forwarder")

	forwarder := &RBForwarder{
		backend: &backend{
			decoderPool:   make(chan chan *Message, workers),
			processorPool: make(chan chan *Message, workers),
			encoderPool:   make(chan chan *Message, workers),
			senderPool:    make(chan chan *Message, workers),

			messages: make(chan *Message, queueSize),
			reports:  make(chan *Message, queueSize),

			messagePool: make(chan *Message, queueSize),
		},
		workers: workers,
	}

	forwarder.close = make(chan struct{})

	for i := 0; i < queueSize; i++ {
		forwarder.backend.messagePool <- &Message{
			Metadata:     make(map[string]interface{}),
			InputBuffer:  new(bytes.Buffer),
			OutputBuffer: new(bytes.Buffer),

			backend: forwarder.backend,
		}
	}

	fields := logrus.Fields{
		"workers":    workers,
		"queue_size": queueSize,
	}
	logger.WithFields(fields).Info("Initialized rB Forwarder")

	return forwarder
}

// Start spawn the workers
func (f *RBForwarder) Start() {
	// Start listening
	f.backend.source.Listen(f)
	logger.Info("Source ready")

	for i := 0; i < f.workers; i++ {
		f.backend.startDecoder(i)
		f.backend.startProcessor(i)
		f.backend.startEncoder(i)
		f.backend.startSender(i)
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
	message = <-f.backend.messagePool
	return
}

// GetReports is used by the source to get a report for a sent message.
// Reports are delivered on the same order that was sent
func (f *RBForwarder) GetReports() <-chan Report {
	f.reports = make(chan Report)
	var currentid int64

	go func() {
		for message := range f.backend.reports {
			report := message.report
			if report.ID == f.backend.currentProcessedID {
				if report.StatusCode == 0 {
					// Success
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
				} else {
					// Fail
					if f.retries < 0 || report.Retries < f.retries {
						// Retry this message
						report.Retries++
						message.Produce()
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
					}
				}

				select {
				case f.reports <- report:
					f.backend.currentProcessedID++
				case <-time.After(1 * time.Second):
					logger.Error("Error on report: Full queue")
				}
			} else {
				// Requeue the report if is not the expected
				if currentid != f.backend.currentProcessedID {
					logger.Warnf("Expected report [%d], got [%d]",
						f.backend.currentProcessedID, report.ID)
					currentid = f.backend.currentProcessedID
				}
				select {
				case f.backend.reports <- message:
				case <-time.After(1 * time.Second):
					logger.Error("Error on report: Full queue")
				}
			}
		}
	}()

	return f.reports
}
