package batcher

import (
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/utils"
)

// Batcher allows to merge multiple messages in a single one
type Batcher struct {
	id           int // Worker ID
	wg           sync.WaitGroup
	batches      map[string]*Batch // Collection of batches pending
	readyBatches chan *Batch
	clk          clock.Clock
	incoming     chan struct {
		m    *utils.Message
		next utils.Next
	}

	Config Config // Batcher configuration
}

// Init starts a gorutine that can receive:
// - New messages that will be added to a existing or new batch of messages
// - A batch of messages that is ready to send (i.e. batch timeout has expired)
func (b *Batcher) Init(id int) {
	b.id = id
	b.batches = make(map[string]*Batch)
	b.readyBatches = make(chan *Batch)
	b.incoming = make(chan struct {
		m    *utils.Message
		next utils.Next
	})
	if b.clk == nil {
		b.clk = clock.New()
	}

	go func() {
		for {
			select {
			case message := <-b.incoming:
				group, exists := message.m.Opts["batch_group"].(string)
				if !exists {
					message.next(message.m)
				} else {
					if batch, exists := b.batches[group]; exists {
						batch.Add(message.m)

						if batch.MessageCount >= b.Config.Limit {
							batch.Send(func() {
								delete(b.batches, group)
								batch.Timer.Stop()
								batch.Next(batch.Message)
							})
						}
					} else {
						b.batches[group] = NewBatch(message.m, group, message.next, b.clk,
							b.Config.TimeoutMillis, b.readyBatches)
					}
				}

				b.wg.Done()

			case batch := <-b.readyBatches:
				batch.Send(func() {
					delete(b.batches, batch.Group)
					batch.Next(batch.Message)
				})
			}
		}
	}()
}

// OnMessage is called when a new message is receive. Add the new message to
// a batch
func (b *Batcher) OnMessage(m *utils.Message, next utils.Next, done utils.Done) {
	b.wg.Add(1)
	b.incoming <- struct {
		m    *utils.Message
		next utils.Next
	}{m, next}
	b.wg.Wait()
}
