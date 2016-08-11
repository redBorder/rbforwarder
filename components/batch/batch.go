package batcher

import (
	"bytes"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/utils"
)

// Batch groups multiple messages
type Batch struct {
	Group        string
	Message      *utils.Message
	Buff         *bytes.Buffer
	MessageCount uint       // Current number of messages in the buffer
	Next         utils.Next // Call to pass the message to the next handler
}

// NewBatch creates a new instance of Batch
func NewBatch(m *utils.Message, group string, next utils.Next, clk clock.Clock,
	timeoutMillis uint, ready chan *Batch) *Batch {
	payload, _ := m.PopPayload()
	b := &Batch{
		Group:        group,
		Next:         next,
		Message:      m,
		MessageCount: 1,
		Buff:         bytes.NewBuffer(payload),
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
	b.Message.PushPayload(b.Buff.Bytes())
	cb()
	b.Next(b.Message)
}

// Add merges a new message in the buffer
func (b *Batch) Add(m *utils.Message) {
	newReport := m.Reports.Pop()
	b.Message.Reports.Push(newReport)

	newPayload, _ := m.PopPayload()
	b.Buff.Write(newPayload)

	b.MessageCount++
}
