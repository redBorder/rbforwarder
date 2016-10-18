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
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/utils"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
)

type Nexter struct {
	mock.Mock
	nextCalled chan *utils.Message
}

func (n *Nexter) Done(m *utils.Message, code int, status string) {
	n.Called(m, code, status)
	n.nextCalled <- m
}

func TestHTTPSender(t *testing.T) {
	Convey("Given an Limiter with 100 messages per second without burst", t, func() {
		limiter := &Limiter{
			Config: Config{
				MessageLimit: 100,
				Burst:        1,
			},
			clk: clock.NewMock(),
		}
		limiter.Spawn(0)

		Convey("When the numer of worker is requested", func() {
			workers := limiter.Workers()

			Convey("Should be only one", func() {
				So(workers, ShouldEqual, 1)
			})
		})

		Convey("When the limit number of messages are reached", func() {
			clk := limiter.clk.(*clock.Mock)
			n := Nexter{
				nextCalled: make(chan *utils.Message, limiter.Config.MessageLimit*2),
			}
			n.On("Done", mock.AnythingOfType("*utils.Message"), 0, "")

			for i := uint64(0); i < limiter.Config.MessageLimit; i++ {
				limiter.OnMessage(nil, n.Done)
			}

			Convey("Then the limiter should be paused", func() {
				So(limiter.currentMessages, ShouldEqual, limiter.Config.MessageLimit)
				So(limiter.paused, ShouldBeTrue)
			})

			Convey("Then after 1 second the limiter should be ready again", func() {
				clk.Add(1 * time.Second)
				limiter.OnMessage(nil, n.Done)
				So(limiter.currentMessages, ShouldEqual, 1)
				So(limiter.paused, ShouldBeFalse)
			})
		})
	})

	Convey("Given an Limiter with 1000 bytes per second without burst", t, func() {
		limiter := &Limiter{
			Config: Config{
				BytesLimit: 1000,
				Burst:      1,
			},
			clk: clock.NewMock(),
		}
		limiter.Spawn(0)

		Convey("When messages are sent", func() {
			n := Nexter{
				nextCalled: make(chan *utils.Message, 100),
			}
			n.On("Done", mock.AnythingOfType("*utils.Message"), 0, "")

			Convey("Then the limiter should not be paused after 750 bytes", func() {
				for i := uint64(0); i < 3; i++ {
					m := utils.NewMessage()
					payload := make([]byte, 250)
					m.PushPayload(payload)
					limiter.OnMessage(m, n.Done)
				}

				So(limiter.currentBytes, ShouldEqual, 750)
				So(limiter.paused, ShouldBeFalse)
			})

			Convey("Then the limiter should be paused after 1000 bytes", func() {
				for i := uint64(0); i < 4; i++ {
					m := utils.NewMessage()
					payload := make([]byte, 250)
					m.PushPayload(payload)
					limiter.OnMessage(m, n.Done)
				}

				So(limiter.currentBytes, ShouldEqual, 1000)
				So(limiter.paused, ShouldBeTrue)
			})

			Convey("Then after 1 second the limiter should be ready again", func() {
				clk := limiter.clk.(*clock.Mock)
				clk.Add(1 * time.Second)

				m := utils.NewMessage()
				payload := make([]byte, 250)
				m.PushPayload(payload)
				limiter.OnMessage(m, n.Done)

				So(limiter.currentBytes, ShouldEqual, 250)
				So(limiter.paused, ShouldBeFalse)
			})
		})
	})

	Convey("Given a limiter with burst", t, func() {
		limiter := &Limiter{
			Config: Config{
				MessageLimit: 100,
				Burst:        2,
			},
			clk: clock.NewMock(),
		}
		limiter.Spawn(0)

		clk := limiter.clk.(*clock.Mock)
		clk.Add(0)
		clk.Add(2 * time.Second)

		Convey("When the limit number of messages are reached", func() {
			n := Nexter{
				nextCalled: make(chan *utils.Message, limiter.Config.MessageLimit*2),
			}
			n.On("Done", mock.AnythingOfType("*utils.Message"), 0, "")

			for i := uint64(0); i < limiter.Config.MessageLimit; i++ {
				limiter.OnMessage(nil, n.Done)
			}

			Convey("Then should be 2 burst available", func() {
				So(len(limiter.keepSending), ShouldEqual, 2)
			})
			Convey("Then messages are not blocked after the limit", func() {
				for i := uint64(0); i < limiter.Config.MessageLimit; i++ {
					limiter.OnMessage(nil, n.Done)
				}
				So(limiter.currentMessages, ShouldEqual, 100)
			})
			Convey("Then the limiter blocks again after reaching limit a second time", func() {
				So(limiter.paused, ShouldBeTrue)
			})
		})
	})
}
