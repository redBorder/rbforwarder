package limiter

import (
	"time"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/utils"
)

// Limiter is a component that blocks the pipeline to ensure a maximum number
// of messages are being processed in a time. You may spawn ONLY ONE worker
type Limiter struct {
	id              int
	currentMessages uint64
	currentBytes    uint64
	config          Config
	keepSending     chan struct{}
	paused          bool
	clk             clock.Clock
}

// Init initializes the limiter
func (l *Limiter) Init(id int) {
	l.id = id
	l.keepSending = make(chan struct{}, l.config.Burst)
	l.paused = false

	go func() {
		for {
			<-l.clk.Timer(1 * time.Second).C
			l.keepSending <- struct{}{}
		}
	}()
}

// OnMessage will block the pipeline when the message rate is too high
func (l *Limiter) OnMessage(m *utils.Message, next utils.Next, done utils.Done) {
	if l.paused {
		<-l.keepSending
		l.currentMessages = 0
		l.currentBytes = 0
		l.paused = false
	}

	l.currentMessages++
	if l.config.BytesLimit > 0 {
		if payload, err := m.PopPayload(); err == nil {
			l.currentBytes += uint64(len(payload))
			m.PushPayload(payload)
		}
	}
	done(m, 0, "")

	if l.config.MessageLimit > 0 && l.currentMessages >= l.config.MessageLimit ||
		l.config.BytesLimit > 0 && l.currentBytes >= l.config.BytesLimit {
		l.paused = true
	}
}
