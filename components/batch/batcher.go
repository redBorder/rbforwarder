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

package batcher

import (
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/utils"
	"github.com/streamrail/concurrent-map"
)

// Batcher allows to merge multiple messages in a single one
type Batcher struct {
	id           int // Worker ID
	batches      cmap.ConcurrentMap
	readyBatches chan *Batch
	clk          clock.Clock
	finished     chan struct{}
	incoming     chan struct {
		m    *utils.Message
		done utils.Done
	}

	Config
}

// Workers returns the number of workers
func (batcher *Batcher) Workers() int {
	return batcher.Config.Workers
}

// Spawn starts a gorutine that can receive:
// - New messages that will be added to a existing or new batch of messages
// - A batch of messages that is ready to send (i.e. batch timeout has expired)
func (batcher *Batcher) Spawn(id int) utils.Composer {
	var wg sync.WaitGroup

	b := *batcher

	b.id = id
	b.batches = cmap.New()
	b.readyBatches = make(chan *Batch)
	b.finished = make(chan struct{})
	b.incoming = make(chan struct {
		m    *utils.Message
		done utils.Done
	})
	if b.clk == nil {
		b.clk = clock.New()
	}

	wg.Add(1)
	go func() {
		wg.Done()
		for {
			select {
			case message := <-b.incoming:
				if !message.m.Opts.Has("batch_group") {
					message.done(message.m, 0, "")
				} else {
					tmp, _ := message.m.Opts.Get("batch_group")
					group := tmp.(string)

					if tmp, exists := b.batches.Get(group); exists {
						batch := tmp.(*Batch)
						batch.Add(message.m)

						if batch.MessageCount >= b.Config.Limit && !batch.Sent {
							batch.Send(func() {
								b.batches.Remove(group)
								batch.Done(batch.Message, 0, "limit")
								batch.Sent = true
							})
						}
					} else {
						b.batches.Set(group, NewBatch(message.m, group, b.Config.Deflate,
							message.done, b.clk, b.Config.TimeoutMillis, b.readyBatches))
					}
				}

				b.finished <- struct{}{}

			case batch := <-b.readyBatches:
				if !batch.Sent {
					batch.Send(func() {
						b.batches.Remove(batch.Group)
						batch.Done(batch.Message, 0, "timeout")
						batch.Sent = true
					})
				}
			}
		}
	}()

	wg.Wait()
	return &b
}

// OnMessage is called when a new message is receive. Add the new message to
// a batch
func (batcher *Batcher) OnMessage(m *utils.Message, done utils.Done) {
	batcher.incoming <- struct {
		m    *utils.Message
		done utils.Done
	}{m, done}

	<-batcher.finished
}
