package rbforwarder

import (
	"bytes"

	"github.com/redBorder/rbforwarder/pipeline"
)

// Backend orchestrates the pipeline
type Backend struct {
	sender pipeline.Sender

	senderPool chan chan *pipeline.Message

	currentProducedID uint64

	input       chan *pipeline.Message
	messages    chan *pipeline.Message
	reports     chan *pipeline.Message
	messagePool chan *pipeline.Message

	workers   int
	queueSize int
}

// NewBackend creates a new Backend
func NewBackend(workers, queueSize, maxMessages, maxBytes int) *Backend {
	b := &Backend{
		workers:   workers,
		queueSize: queueSize,
	}

	b.senderPool = make(chan chan *pipeline.Message, b.workers)

	b.messages = make(chan *pipeline.Message)
	b.input = make(chan *pipeline.Message)
	b.reports = make(chan *pipeline.Message)
	b.messagePool = make(chan *pipeline.Message, b.queueSize)

	for i := 0; i < b.queueSize; i++ {
		b.messagePool <- &pipeline.Message{
			InputBuffer:  new(bytes.Buffer),
			OutputBuffer: new(bytes.Buffer),
		}
	}

	return b
}

// Init initializes a backend
func (b *Backend) Init() {
	for i := 0; i < b.workers; i++ {
		b.startSender(i)
	}

	// Get messages from produces and send them to workers
	done := make(chan struct{})
	go func() {
		done <- struct{}{}
		for m := range b.input {
			messageChannel := <-b.senderPool
			messageChannel <- m
		}
	}()
	<-done

	Logger.Debug("Backend ready")
}

// Worker that sends the message
func (b *Backend) startSender(i int) {
	sender := b.sender
	sender.Init(i, b.reports)

	workerChannel := make(chan *pipeline.Message)

	go func() {
		for {
			b.senderPool <- workerChannel
			message := <-workerChannel
			sender.OnMessage(message)
		}
	}()
}
