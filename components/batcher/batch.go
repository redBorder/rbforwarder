package batcher

import (
	"bytes"
	"errors"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/oleiade/lane"
	"github.com/redBorder/rbforwarder/types"
)

// BatchMessage contains multiple messages
type BatchMessage struct {
	Group        string        // Name used to group messages
	Buff         *bytes.Buffer // Buffer for merge multiple messages
	MessageCount uint          // Current number of messages in the buffer
	BytesCount   uint          // Current number of bytes in the buffer
	Next         types.Next    // Call to pass the message to the next handler
	Opts         *lane.Stack   // Original messages options
}

// StartTimeout initializes a timeout used to send the messages when expires. No
// matters how many messages are in the buffer.
func (b *BatchMessage) StartTimeout(clk clock.Clock, timeoutMillis uint, ready chan *BatchMessage) *BatchMessage {
	if clk == nil {
		clk = clock.New()
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
func (b *BatchMessage) Send(cb func()) {
	cb()
	b.Next(b)
}

// Write merges a new message in the buffer
func (b *BatchMessage) Write(data []byte) {
	b.Buff.Write(data)
	b.MessageCount++
}

// PopData returns the buffer with all the messages merged
func (b *BatchMessage) PopData() (ret []byte, err error) {
	return b.Buff.Bytes(), nil
}

// PopOpts get the data stored by the previous handler
func (b *BatchMessage) PopOpts() (ret map[string]interface{}, err error) {
	if b.Opts.Empty() {
		err = errors.New("Empty stack")
		return
	}

	ret = b.Opts.Pop().(map[string]interface{})

	return
}

// Reports do nothing
func (b *BatchMessage) Reports() []types.Reporter {
	return nil
}
