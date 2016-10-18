// Copyright (C) ENEO Tecnologia SL - 2016
//
// Authors: Diego Fernández Barrera <dfernandez@redborder.com> <bigomby@gmail.com>
// 					Eugenio Pérez Martín <eugenio@redborder.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/lgpl-3.0.txt>.

package rbforwarder

import (
	"github.com/oleiade/lane"
	"github.com/redBorder/rbforwarder/utils"
)

// Component contains information about a pipeline component
type Component struct {
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
func (p *pipeline) PushComponent(composser utils.Composer) {
	p.components = append(p.components, struct {
		composser utils.Composer
		pool      chan chan *utils.Message
	}{
		composser: composser,
		pool:      make(chan chan *utils.Message, composser.Workers()),
	})
}

func (p *pipeline) Run() {
	for index, component := range p.components {
		for w := 0; w < component.composser.Workers(); w++ {
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
							if code == 0 && len(p.components)-1 > index {
								nextWorker := <-p.components[index+1].pool
								nextWorker <- m
							} else {
								reports := lane.NewStack()

								for !m.Reports.Empty() {
									rep := m.Reports.Pop().(Report)
									rep.Component = index
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
			case m, ok := <-p.retry:
				if ok {
					rep := m.Reports.Head().(Report)
					worker := <-p.components[rep.Component].pool
					worker <- m
				}
				continue

			default:
			}

			select {
			case m, ok := <-p.retry:
				if ok {
					rep := m.Reports.Head().(Report)
					worker := <-p.components[rep.Component].pool
					worker <- m
				}
				continue

			case m, ok := <-p.input:
				// If input channel has been closed, close output channel
				if !ok {
					for _, component := range p.components {
						for i := 0; i < component.composser.Workers(); i++ {
							worker := <-component.pool
							close(worker)
						}
					}
					close(p.output)
				} else {
					worker := <-p.components[0].pool
					worker <- m
				}
			}
		}
	}()
}
