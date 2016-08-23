package batcher

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"io"
	"sync"
	"sync/atomic"
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
	MessageCount uint64     // Current number of messages in the buffer
	Done         utils.Done // Call to pass the message to the next handler
	Timer        *clock.Timer
	Sent         bool
}

// NewBatch creates a new instance of Batch
func NewBatch(m *utils.Message, group string, deflate bool, done utils.Done,
	clk clock.Clock, timeoutMillis uint, ready chan *Batch) *Batch {
	var wg sync.WaitGroup

	payload, _ := m.PopPayload()
	b := &Batch{
		Group:        group,
		Deflate:      deflate,
		Done:         done,
		Message:      m,
		MessageCount: 1,
		Buf:          new(bytes.Buffer),
		Sent:         false,
	}

	if b.Deflate {
		b.Writer = zlib.NewWriter(b.Buf)
	} else {
		b.Writer = bufio.NewWriter(b.Buf)
	}

	b.Writer.Write(payload)

	if timeoutMillis > 0 {
		b.Timer = clk.Timer(time.Duration(timeoutMillis) * time.Millisecond)

		wg.Add(1)
		go func() {
			wg.Done()
			<-b.Timer.C
			if atomic.LoadUint64(&b.MessageCount) > 0 {
				ready <- b
			}
		}()
	}

	wg.Wait()
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

	atomic.AddUint64(&b.MessageCount, 1)
}
