package rbforwarder

import "sync"

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

	currentProducedID  int64
	currentProcessedID int64

	messages chan *Message
	reports  chan Report

	messagePool chan *Message
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
