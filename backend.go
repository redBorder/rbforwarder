package rbforwarder

import (
	"time"

	"github.com/redBorder/rbforwarder/pipeline"
)

// Backend orchestrates the pipeline
type Backend struct {
	componentPools []chan chan *message
	input          chan *message
	output         chan *message

	working int
}

// NewBackend creates a new Backend
func NewBackend(input, output chan *message) *Backend {
	b := &Backend{
		input:   input,
		output:  output,
		working: 1,
	}

	go func() {
		// Start receiving messages
		for m := range b.input {
			worker := <-b.componentPools[0]
			worker <- m
		}

		// When a close signal is received clean the workers. Wait for workers to
		// terminate
		b.working = 0
		for _, componentPool := range b.componentPools {
		loop:
			for {
				select {
				case worker := <-componentPool:
					close(worker)
				case <-time.After(10 * time.Millisecond):
					break loop
				}
			}
		}

		// Messages coming too late will be ignored. If the channel is not set to
		// nil, late messages will panic
		b.output = nil

		// Send a close signal to message handler
		close(output)
	}()

	return b
}

// PushComponent adds a new component to the pipeline
func (b *Backend) PushComponent(c pipeline.Composer, w int) {
	index := len(b.componentPools)
	componentPool := make(chan chan *message, w)
	b.componentPools = append(b.componentPools, componentPool)

	for i := 0; i < w; i++ {
		c.Init(i)

		worker := make(chan *message)
		b.componentPools[index] <- worker

		go func() {
			for m := range worker {
				c.OnMessage(
					m,
					func(m pipeline.Messenger) {
						if len(b.componentPools) >= index {
							nextWorker := <-b.componentPools[index+1]
							nextWorker <- m.(*message)
						}
					},
					func(m pipeline.Messenger, code int, status string) {
						rbmessage := m.(*message)
						rbmessage.code = code
						rbmessage.status = status
						b.output <- rbmessage
					},
				)

				if b.working == 1 {
					b.componentPools[index] <- worker
				}
			}
		}()
	}
}
