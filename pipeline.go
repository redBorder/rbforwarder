package rbforwarder

import (
	"sync"

	"github.com/redBorder/rbforwarder/types"
)

type component struct {
	pool    chan chan *message
	workers int
}

// pipeline contains the components
type pipeline struct {
	components []component
	input      chan *message
	retry      chan *message
	output     chan *message
}

// newPipeline creates a new Backend
func newPipeline(input, retry, output chan *message) *pipeline {
	var wg sync.WaitGroup
	p := &pipeline{
		input:  input,
		retry:  retry,
		output: output,
	}

	wg.Add(1)
	go func() {
		wg.Done()

		for {
			select {
			case m, ok := <-p.input:
				// If input channel has been closed, close output channel
				if !ok {
					for _, component := range p.components {
						for i := 0; i < component.workers; i++ {
							worker := <-component.pool
							close(worker)
						}
					}
					close(p.output)
				} else {
					worker := <-p.components[0].pool
					worker <- m
				}

			case m := <-p.retry:
				worker := <-p.components[0].pool
				worker <- m
			}
		}
	}()

	wg.Wait()
	return p
}

// PushComponent adds a new component to the pipeline
func (p *pipeline) PushComponent(composser types.Composer, w int) {
	var wg sync.WaitGroup
	c := component{
		workers: w,
		pool:    make(chan chan *message, w),
	}

	index := len(p.components)
	p.components = append(p.components, c)

	for i := 0; i < w; i++ {
		composser.Init(i)

		worker := make(chan *message)
		p.components[index].pool <- worker

		wg.Add(1)
		go func(i int) {
			wg.Done()
			for m := range worker {
				composser.OnMessage(m, func(m types.Messenger) {
					if len(p.components) >= index {
						nextWorker := <-p.components[index+1].pool
						nextWorker <- m.(*message)
					}
				}, func(m types.Messenger, code int, status string) {
					rbmessage := m.(*message)
					rbmessage.code = code
					rbmessage.status = status
					p.output <- rbmessage
				})

				p.components[index].pool <- worker
			}
		}(i)
	}

	wg.Wait()
}
