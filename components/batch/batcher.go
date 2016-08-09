package batcher

import (
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/types"
)

// Batcher allows to merge multiple messages in a single one
type Batcher struct {
	id           int               // Worker ID
	batches      map[string]*Batch // Collection of batches pending
	readyBatches chan *Batch
	wg           sync.WaitGroup
	clk          clock.Clock

	config Config // Batcher configuration
}

// Init starts a gorutine that can receive:
// - New messages that will be added to a existing or new batch of messages
// - A batch of messages that is ready to send (i.e. batch timeout has expired)
func (b *Batcher) Init(id int) {
	b.batches = make(map[string]*Batch)
	b.readyBatches = make(chan *Batch)

	if b.clk == nil {
		b.clk = clock.New()
	}

	go func() {
		for batch := range b.readyBatches {
			batch.Send(func() {
				delete(b.batches, batch.Group)
			})
		}
	}()
}

// OnMessage is called when a new message is receive. Add the new message to
// a batch
func (b *Batcher) OnMessage(m *types.Message, next types.Next, done types.Done) {
	opts := m.Opts.Head().(map[string]interface{})
	group, exists := opts["batch_group"].(string)
	if !exists {
		next(m)
		return
	}

	if batch, exists := b.batches[group]; exists {
		batch.Add(m)
		if batch.MessageCount >= b.config.Limit {
			b.readyBatches <- batch
		}
	} else {
		b.batches[group] = NewBatch(m, group, next, b.clk, b.config.TimeoutMillis, b.readyBatches)
	}
}
