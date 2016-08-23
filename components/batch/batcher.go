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

	Config Config // Batcher configuration
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
