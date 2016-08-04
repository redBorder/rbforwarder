package batcher

import (
	"bytes"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/oleiade/lane"
	"github.com/redBorder/rbforwarder/types"
)

// Batcher allows to merge multiple messages in a single one
type Batcher struct {
	id             int                      // Worker ID
	batches        map[string]*BatchMessage // Collection of batches pending
	newBatches     chan *BatchMessage       // Send messages to sender gorutine
	pendingBatches chan *BatchMessage       // Send messages to sender gorutine
	wg             sync.WaitGroup
	clk            clock.Clock

	config Config // Batcher configuration
}

// Init starts a gorutine that can receive:
// - New messages that will be added to a existing or new batch of messages
// - A batch of messages that is ready to send (i.e. batch timeout has expired)
func (b *Batcher) Init(id int) {
	b.batches = make(map[string]*BatchMessage)
	b.newBatches = make(chan *BatchMessage)
	b.pendingBatches = make(chan *BatchMessage)

	readyBatches := make(chan *BatchMessage)

	if b.clk == nil {
		b.clk = clock.New()
	}

	go func() {
		for {
			select {
			case batchMessage := <-readyBatches:
				batchMessage.Send(func() {
					delete(b.batches, batchMessage.Group)
				})

			case batchMessage := <-b.newBatches:
				batchMessage.StartTimeout(b.clk, b.config.TimeoutMillis, readyBatches)
				b.batches[batchMessage.Group] = batchMessage
				b.wg.Done()

			case batchMessage := <-b.pendingBatches:
				opts, err := batchMessage.PopOpts()
				if err != nil {
					break
				}

				b.batches[batchMessage.Group].Opts.Push(opts)
				b.batches[batchMessage.Group].Write(batchMessage.Buff.Bytes())

				if b.batches[batchMessage.Group].MessageCount >= b.config.Limit {
					b.batches[batchMessage.Group].Send(func() {
						delete(b.batches, batchMessage.Group)
					})
				}

				b.wg.Done()
			}
		}
	}()
}

// OnMessage is called when a new message is receive. Add the new message to
// a batch
func (b *Batcher) OnMessage(m types.Messenger, next types.Next, done types.Done) {
	opts, _ := m.PopOpts()
	group, ok := opts["batch_group"].(string)
	if !ok {
		next(m)
	}

	data, _ := m.PopData()
	batchMessage := &BatchMessage{
		Buff:         bytes.NewBuffer(data),
		Opts:         lane.NewStack(),
		MessageCount: 1,
		Next:         next,
		Group:        group,
	}

	batchMessage.Opts.Push(opts)

	b.wg.Add(1)
	if _, exists := b.batches[group]; exists {
		b.pendingBatches <- batchMessage
	} else {
		b.newBatches <- batchMessage
	}

	b.wg.Wait()
}
