package rbforwarder

import (
	"sync"

	"github.com/oleiade/lane"
	"github.com/redBorder/rbforwarder/utils"
)

type component struct {
	pool    chan chan *utils.Message
	workers int
}

// pipeline contains the components
type pipeline struct {
	components []component
	input      chan *utils.Message
	retry      chan *utils.Message
	output     chan *utils.Message
}

// newPipeline creates a new Backend
func newPipeline(input, retry, output chan *utils.Message) *pipeline {
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
func (p *pipeline) PushComponent(composser utils.Composer, w int) {
	var wg sync.WaitGroup
	c := component{
		workers: w,
		pool:    make(chan chan *utils.Message, w),
	}

	index := len(p.components)
	p.components = append(p.components, c)

	for i := 0; i < w; i++ {
		composser.Init(i)

		worker := make(chan *utils.Message)
		p.components[index].pool <- worker

		wg.Add(1)
		go func(i int) {
			wg.Done()
			for m := range worker {
				composser.OnMessage(m, func(m *utils.Message) {
					if len(p.components) >= index {
						nextWorker := <-p.components[index+1].pool
						nextWorker <- m
					}
				}, func(m *utils.Message, code int, status string) {
					reports := lane.NewStack()

					for !m.Reports.Empty() {
						rep := m.Reports.Pop().(Report)
						rep.Code = code
						rep.Status = status
						reports.Push(rep)
					}

					m.Reports = reports
					p.output <- m
				})

				p.components[index].pool <- worker
			}
		}(i)
	}

	wg.Wait()
}
