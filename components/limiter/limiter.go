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
	keepSending     chan struct{}
	paused          bool
	clk             clock.Clock

	Config
}

// Workers returns 1 because it should be only one instance of this component
func (l *Limiter) Workers() int {
	return 1
}

// Spawn initializes the limiter
func (l *Limiter) Spawn(id int) utils.Composer {
	l.keepSending = make(chan struct{}, l.Config.Burst)
	if l.clk == nil {
		l.clk = clock.New()
	}

	go func() {
		for {
			<-l.clk.Timer(1 * time.Second).C
			l.keepSending <- struct{}{}
		}
	}()

	return l
}

// OnMessage will block the pipeline when the message rate is too high
func (l *Limiter) OnMessage(m *utils.Message, done utils.Done) {
	if l.paused {
		<-l.keepSending
		l.currentMessages = 0
		l.currentBytes = 0
		l.paused = false
	}

	l.currentMessages++
	if l.Config.BytesLimit > 0 {
		if payload, err := m.PopPayload(); err == nil {
			l.currentBytes += uint64(len(payload))
			m.PushPayload(payload)
		}
	}
	done(m, 0, "")

	if l.Config.MessageLimit > 0 && l.currentMessages >= l.Config.MessageLimit ||
		l.Config.BytesLimit > 0 && l.currentBytes >= l.Config.BytesLimit {
		l.paused = true
	}
}
