package rbforwarder

import (
	"sync/atomic"
	"time"

	"github.com/redBorder/rbforwarder/types"
)

// pipeline contains the components
type pipeline struct {
	componentPools []chan chan *message
	input          chan *message
	output         chan *message

	working int32
}

// newPipeline creates a new Backend
func newPipeline(input, output chan *message) *pipeline {
	p := &pipeline{
		input:   input,
		output:  output,
		working: 1,
	}

	go func() {
		// Start receiving messages
		for m := range p.input {
			worker := <-p.componentPools[0]
			worker <- m
		}

		// When a close signal is received clean the workers. Wait for workers to
		// terminate
		atomic.StoreInt32(&p.working, 0)
		for _, componentPool := range p.componentPools {
		loop:
			for {
				select {
				case worker := <-componentPool:
					close(worker)
				case <-time.After(2000 * time.Millisecond):
					break loop
				}
			}
		}

		// Messages coming too late will be ignored. If the channel is not set to
		// nil, late messages will panic
		p.output = nil

		// Send a close signal to message handler
		close(output)
	}()

	return p
}

// PushComponent adds a new component to the pipeline
func (p *pipeline) PushComponent(c types.Composer, w int) {
	index := len(p.componentPools)
	componentPool := make(chan chan *message, w)
	p.componentPools = append(p.componentPools, componentPool)

	for i := 0; i < w; i++ {
		c.Init(i)

		worker := make(chan *message)
		p.componentPools[index] <- worker

		go func() {
			for m := range worker {
				c.OnMessage(
					m,
					func(m types.Messenger) {
						if len(p.componentPools) >= index {
							nextWorker := <-p.componentPools[index+1]
							nextWorker <- m.(*message)
						}
					},
					func(m types.Messenger, code int, status string) {
						rbmessage := m.(*message)
						rbmessage.code = code
						rbmessage.status = status
						p.output <- rbmessage
					},
				)

				if atomic.LoadInt32(&p.working) == 1 {
					p.componentPools[index] <- worker
				}
			}
		}()
	}
}
