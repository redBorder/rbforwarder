package rbforwarder

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

type backend struct {
	source       Source
	decoder      Decoder
	processor    Processor
	encoder      Encoder
	senderHelper SenderHelper

	// Pool of workers
	decoderPool   chan chan *Message
	processorPool chan chan *Message
	encoderPool   chan chan *Message
	senderPool    chan chan *Message

	currentProducedID  uint64
	currentProcessedID uint64

	messages    chan *Message
	reports     chan *Message
	messagePool chan *Message

	workers int
	retries int
	closed  bool
}

// Worker that decodes the received message
func (b *backend) startDecoder(i int) {
	if b.decoder != nil {
		b.decoder.Init(i)
	}
	workerChannel := make(chan *Message)

	go func() {
		for {
			// The worker is ready, put himself on the worker pool
			b.decoderPool <- workerChannel

			// Wait for a new message
			message := <-workerChannel

			// Perform work on the message
			if b.decoder != nil {
				b.decoder.Decode(message)
			}

			// Get a worker for the next element on the pipe
			messageChannel := <-b.processorPool

			// Send the message to the next worker
			messageChannel <- message
		}
	}()
}

// Worker that performs modifications on a decoded message
func (b *backend) startProcessor(i int) {
	if b.processor != nil {
		b.processor.Init(i)
	}
	workerChannel := make(chan *Message)

	// The worker is ready, put himself on the worker pool
	go func() {
		for {
			// The worker is ready, put himself on the worker pool
			b.processorPool <- workerChannel

			// Wait for a new message
			message := <-workerChannel

			// Perform work on the message
			if b.processor != nil {
				b.processor.Process(message)
			}

			// Get a worker for the next element on the pipe
			messageChannel := <-b.encoderPool

			// Send the message to the next worker
			messageChannel <- message
		}
	}()
}

// Worker that encodes a modified message
func (b *backend) startEncoder(i int) {
	if b.encoder != nil {
		b.encoder.Init(i)
	}
	workerChannel := make(chan *Message)

	go func() {
		for {
			// The worker is ready, put himself on the worker pool
			b.encoderPool <- workerChannel

			// Wait for a new message
			message := <-workerChannel

			// Perform work on the message
			if b.encoder != nil {
				b.encoder.Encode(message)
			} else {
				message.OutputBuffer = message.InputBuffer
			}

			// Get a worker for the next element on the pipe
			messageChannel := <-b.senderPool

			// Send the message to the next worker
			messageChannel <- message
		}
	}()
}

// Worker that sends the message
func (b *backend) startSender(i int) {
	if b.senderHelper == nil {
		logger.Fatal("No sender provided")
	}

	sender := b.senderHelper.CreateSender()
	sender.Init(i)

	workerChannel := make(chan *Message)

	go func() {
		for {
			b.senderPool <- workerChannel
			message := <-workerChannel
			sender.Send(message)
		}
	}()
}
