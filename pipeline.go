package rbforwarder

import (
	"github.com/oleiade/lane"
	"github.com/redBorder/rbforwarder/utils"
)

// Component contains information about a pipeline component
type Component struct {
	workers   int
	composser utils.Composer
	pool      chan chan *utils.Message
}

// pipeline contains the components
type pipeline struct {
	components []Component
	input      chan *utils.Message
	retry      chan *utils.Message
	output     chan *utils.Message
}

// newPipeline creates a new Backend
func newPipeline(input, retry, output chan *utils.Message) *pipeline {
	return &pipeline{
		input:  input,
		retry:  retry,
		output: output,
	}
}

// PushComponent adds a new component to the pipeline
func (p *pipeline) PushComponent(composser utils.Composer, w int) {
	p.components = append(p.components, struct {
		workers   int
		composser utils.Composer
		pool      chan chan *utils.Message
	}{
		workers:   w,
		composser: composser,
		pool:      make(chan chan *utils.Message, w),
	})
}

func (p *pipeline) Run() {
	for index, component := range p.components {
		for w := 0; w < component.workers; w++ {
			go func(w, index int, component Component) {
				component.composser = component.composser.Spawn(w)
				messages := make(chan *utils.Message)
				component.pool <- messages

				for m := range messages {
					component.composser.OnMessage(m,
						// Done function
						func(m *utils.Message, code int, status string) {
							// If there is another component next in the pipeline send the
							// messate to it. I other case send the message to the report
							// handler
							if len(p.components)-1 > index {
								nextWorker := <-p.components[index+1].pool
								nextWorker <- m
							} else {
								reports := lane.NewStack()

								for !m.Reports.Empty() {
									rep := m.Reports.Pop().(Report)
									rep.Code = code
									rep.Status = status
									reports.Push(rep)
								}

								m.Reports = reports
								p.output <- m
							}
						})

					component.pool <- messages
				}
			}(w, index, component)
		}
	}

	go func() {
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
}
