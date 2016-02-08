package pipe

import (
	"os"
	"os/signal"

	"github.com/redBorder/rb-forwarder/decoders"
	"github.com/redBorder/rb-forwarder/encoders"
	"github.com/redBorder/rb-forwarder/listeners"
	"github.com/redBorder/rb-forwarder/processors"
	"github.com/redBorder/rb-forwarder/senders"
	"github.com/redBorder/rb-forwarder/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Entry

// Defines a collection of actions that act in cascade starting with an incoming message
type Pipe struct {

	// Configuration
	config util.PipeConfig

	// Pool of workers
	DecoderPool   chan chan *util.Message
	ProcessorPool chan chan *util.Message
	EncoderPool   chan chan *util.Message
	SenderPool    chan chan *util.Message
}

/**
 * Creates a new pipe using given processors and a number of workers
 */
func NewPipe(config util.PipeConfig, workers int) Pipe {

	log = util.NewLogger("pipe")

	p := Pipe{
		config:        config,
		DecoderPool:   make(chan chan *util.Message, workers),
		ProcessorPool: make(chan chan *util.Message, workers),
		EncoderPool:   make(chan chan *util.Message, workers),
		SenderPool:    make(chan chan *util.Message, workers),
	}

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)

	listener := listeners.NewListener(p.config.Listener)
	c := listener.Listen()

	log.Info("Listener ready")
	for i := 0; i < workers; i++ {
		p.startDecoder(i)
		p.startProcessor(i)
		p.startEncoder(i)
		p.startSender(i)
	}

	for {
		select {
		case message := <-c:
			messageChannel := <-p.DecoderPool
			messageChannel <- message
			break
		case <-ctrlc:
			listener.Close()
			os.Exit(0)
			break
		}
	}

	return p
}

// Worker that decodes the received message
func (p *Pipe) startDecoder(i int) {
	decoder := decoders.NewDecoder(p.config.Decoder)
	decoder.Init(i)
	workerChannel := make(chan *util.Message)

	go func() {
		for {

			// The worker is ready, put himself on the worker pool
			p.DecoderPool <- workerChannel

			// Wait for a new message
			message := <-workerChannel

			// Perform work on the message
			decoder.Decode(message)

			// Get a worker for the next element on the pipe
			messageChannel := <-p.ProcessorPool

			// Send the message to the next worker
			messageChannel <- message
		}
	}()
}

// Worker that performs modifications on a decoded message
func (p *Pipe) startProcessor(i int) {
	processor := processors.NewProcessor(p.config.Processor)
	processor.Init(i)
	workerChannel := make(chan *util.Message)

	// The worker is ready, put himself on the worker pool
	go func() {
		for {

			// The worker is ready, put himself on the worker pool
			p.ProcessorPool <- workerChannel

			// Wait for a new message
			message := <-workerChannel

			// Perform work on the message
			processor.Process(message)

			// Get a worker for the next element on the pipe
			messageChannel := <-p.EncoderPool

			// Send the message to the next worker
			messageChannel <- message
		}
	}()
}

// Worker that encodes a modified message
func (p *Pipe) startEncoder(i int) {
	encoder := encoders.NewEncoder(p.config.Encoder)
	encoder.Init(i)
	workerChannel := make(chan *util.Message)

	go func() {
		for {

			// The worker is ready, put himself on the worker pool
			p.EncoderPool <- workerChannel

			// Wait for a new message
			message := <-workerChannel

			// Perform work on the message
			encoder.Encode(message)

			// Get a worker for the next element on the pipe
			messageChannel := <-p.SenderPool

			// Send the message to the next worker
			messageChannel <- message
		}
	}()
}

// Worker that sends the message
func (p *Pipe) startSender(i int) {
	sender := senders.NewSender(p.config.Sender)
	sender.Init(i)
	workerChannel := make(chan *util.Message)

	go func() {
		for {
			p.SenderPool <- workerChannel
			message := <-workerChannel
			sender.Send(message)
		}
	}()
}
