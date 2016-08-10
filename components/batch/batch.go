package batcher

import (
	"bytes"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/types"
)

// Batch groups multiple messages
type Batch struct {
	Group        string
	Message      *types.Message
	MessageCount uint       // Current number of messages in the buffer
	Next         types.Next // Call to pass the message to the next handler
}

// NewBatch creates a new instance of Batch
func NewBatch(m *types.Message, group string, next types.Next, clk clock.Clock,
	timeoutMillis uint, ready chan *Batch) *Batch {
	b := &Batch{
		Group:        group,
		Next:         next,
		Message:      m,
		MessageCount: 1,
	}

	if timeoutMillis != 0 {
		timer := clk.Timer(time.Duration(timeoutMillis) * time.Millisecond)

		go func() {
			<-timer.C
			if b.MessageCount > 0 {
				ready <- b
			}
		}()
	}

	return b
}

// Send the batch of messages to the next handler in the pipeline
func (b *Batch) Send(cb func()) {
	cb()
	b.Next(b.Message)
}

// Add merges a new message in the buffer
func (b *Batch) Add(m *types.Message) {
	newPayload := m.Payload.Pop().([]byte)
	newOptions := m.Opts.Pop().(map[string]interface{})
	newReport := m.Reports.Pop()

	currentPayload := b.Message.Payload.Pop().([]byte)
	buff := bytes.NewBuffer(currentPayload)
	buff.Write(newPayload)

	b.Message.Payload.Push(buff.Bytes())
	b.Message.Opts.Push(newOptions)
	b.Message.Reports.Push(newReport)

	b.MessageCount++
}
