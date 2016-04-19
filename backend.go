package rbforwarder

import (
	"bytes"
	"time"
)

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

type backend struct {
	decoder      Decoder
	processor    Processor
	encoder      Encoder
	senderHelper SenderHelper

	// Pool of workers
	decoderPool   chan chan *Message
	processorPool chan chan *Message
	encoderPool   chan chan *Message
	senderPool    chan chan *Message

	currentProducedID uint64

	input       chan *Message
	messages    chan *Message
	reports     chan *Message
	messagePool chan *Message

	workers     int
	queue       int
	maxMessages int
	maxBytes    int

	active bool

	currentMessages uint64
	currentBytes    uint64
	keepSending     chan struct{}
}

func (b *backend) Init() {
	b.decoderPool = make(chan chan *Message, b.workers)
	b.processorPool = make(chan chan *Message, b.workers)
	b.encoderPool = make(chan chan *Message, b.workers)
	b.senderPool = make(chan chan *Message, b.workers)

	b.messages = make(chan *Message)
	b.input = make(chan *Message)
	b.reports = make(chan *Message)
	b.messagePool = make(chan *Message, b.queue)

	b.keepSending = make(chan struct{})

	for i := 0; i < b.queue; i++ {
		b.messagePool <- &Message{
			Metadata:     make(map[string]interface{}),
			InputBuffer:  new(bytes.Buffer),
			OutputBuffer: new(bytes.Buffer),

			backend: b,
		}
	}

	for i := 0; i < b.workers; i++ {
		b.startDecoder(i)
		b.startProcessor(i)
		b.startEncoder(i)
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
			case messageChannel := <-b.decoderPool:
				select {
				case messageChannel <- m:
					b.currentMessages++
					b.currentBytes += uint64(m.OutputBuffer.Len())
				case <-time.After(1 * time.Second):
					logger.Warn("Error on produce: Full queue")
				}
			case <-time.After(1 * time.Second):
				m.Report(-1, "Error on produce: No workers available")
			}
		}
	}()
	<-done

	b.active = true
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
