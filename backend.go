package rbforwarder

import (
	"bytes"
	"time"

	"github.com/redBorder/rbforwarder/pipeline"
)

// Backend orchestrates the pipeline
type Backend struct {
	sender pipeline.Sender

	// Pool of workers
	senderPool chan chan *pipeline.Message

	currentProducedID uint64

	input       chan *pipeline.Message
	messages    chan *pipeline.Message
	reports     chan *pipeline.Message
	messagePool chan *pipeline.Message

	workers     int
	queueSize   int
	maxMessages int
	maxBytes    int

	active bool

	currentMessages uint64
	currentBytes    uint64
	keepSending     chan struct{}
}

// NewBackend creates a new Backend
func NewBackend(workers, queueSize, maxMessages, maxBytes int) *Backend {
	b := &Backend{
		workers:     workers,
		queueSize:   queueSize,
		maxMessages: maxMessages,
		maxBytes:    maxBytes,
	}

	b.senderPool = make(chan chan *pipeline.Message, b.workers)

	b.messages = make(chan *pipeline.Message)
	b.input = make(chan *pipeline.Message)
	b.reports = make(chan *pipeline.Message)
	b.messagePool = make(chan *pipeline.Message, b.queueSize)

	b.keepSending = make(chan struct{})

	for i := 0; i < b.queueSize; i++ {
		b.messagePool <- &pipeline.Message{
			Metadata:     make(map[string]interface{}),
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

	// Limit the messages/bytes per second
	go func() {
		for {
			timer := time.NewTimer(1 * time.Second)
			<-timer.C
			b.keepSending <- struct{}{}
		}
	}()

	// Get messages from produces
	done := make(chan struct{})
	go func() {
		done <- struct{}{}
		for m := range b.input {

			// Wait if the limit has ben reached
			if b.maxMessages > 0 && b.currentMessages >= uint64(b.maxMessages) {
				<-b.keepSending
				b.currentMessages = 0
			} else if b.maxBytes > 0 && b.currentBytes >= uint64(b.maxBytes) {
				<-b.keepSending
				b.currentBytes = 0
			}

			// Send to workers
			select {
			case messageChannel := <-b.senderPool:
				select {
				case messageChannel <- m:
					b.currentMessages++
					b.currentBytes += uint64(m.InputBuffer.Len())
				case <-time.After(1 * time.Second):
					Logger.Warn("Error on produce: Full queue")
				}
			case <-time.After(1 * time.Second):
				m.Report.StatusCode = -1
				m.Report.Status = "Error on produce: No workers available"
				b.reports <- m
			}
		}
	}()
	<-done

	b.active = true
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
