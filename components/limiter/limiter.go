// Copyright (C) ENEO Tecnologia SL - 2016
//
// Authors: Diego Fernández Barrera <dfernandez@redborder.com> <bigomby@gmail.com>
// 					Eugenio Pérez Martín <eugenio@redborder.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/lgpl-3.0.txt>.

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
