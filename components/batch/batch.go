package batcher

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"io"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/utils"
)

// Batch groups multiple messages
type Batch struct {
	Group        string
	Deflate      bool
	Message      *utils.Message
	Buf          *bytes.Buffer
	Writer       io.Writer
	MessageCount uint       // Current number of messages in the buffer
	Next         utils.Next // Call to pass the message to the next handler
	Timer        *clock.Timer
}

// NewBatch creates a new instance of Batch
func NewBatch(m *utils.Message, group string, deflate bool, next utils.Next,
	clk clock.Clock, timeoutMillis uint, ready chan *Batch) *Batch {
	payload, _ := m.PopPayload()
	b := &Batch{
		Group:        group,
		Deflate:      deflate,
		Next:         next,
		Message:      m,
		MessageCount: 1,
		Buf:          new(bytes.Buffer),
	}

	if b.Deflate {
		b.Writer = zlib.NewWriter(b.Buf)
	} else {
		b.Writer = bufio.NewWriter(b.Buf)
	}

	b.Writer.Write(payload)

	if timeoutMillis != 0 {
		b.Timer = clk.Timer(time.Duration(timeoutMillis) * time.Millisecond)

		go func() {
			<-b.Timer.C
			if b.MessageCount > 0 {
				ready <- b
			}
		}()
	}

	return b
}

// Send the batch of messages to the next handler in the pipeline
func (b *Batch) Send(cb func()) {
	if b.Deflate {
		b.Writer.(*zlib.Writer).Flush()
	} else {
		b.Writer.(*bufio.Writer).Flush()
	}

	b.Message.PushPayload(b.Buf.Bytes())
	cb()
}

// Add merges a new message in the buffer
func (b *Batch) Add(m *utils.Message) {
	newReport := m.Reports.Pop()
	b.Message.Reports.Push(newReport)

	newPayload, _ := m.PopPayload()
	b.Writer.Write(newPayload)

	b.MessageCount++
}
