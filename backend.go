package rbforwarder

import "github.com/redBorder/rbforwarder/pipeline"

// Backend orchestrates the pipeline
type Backend struct {
	sender     pipeline.Sender
	senderPool chan chan *message
	input      chan *message
	workers    int
	queueSize  int
}

// NewBackend creates a new Backend
func NewBackend(workers, queueSize, maxMessages, maxBytes int) *Backend {
	b := &Backend{
		workers:   workers,
		queueSize: queueSize,
	}

	b.senderPool = make(chan chan *message, b.workers)
	b.input = make(chan *message)

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
	sender.Init(i)

	workerChannel := make(chan *message)

	go func() {
		for {
			b.senderPool <- workerChannel
			message := <-workerChannel
			sender.OnMessage(message)
		}
	}()
}
