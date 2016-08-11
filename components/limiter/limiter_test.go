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

func (n *Nexter) Next(m *utils.Message) {
	n.Called(m)
	n.nextCalled <- m
}

func TestHTTPSender(t *testing.T) {
	Convey("Given an Limiter with 100 messages per second without burst", t, func() {
		limiter := &Limiter{
			config: Config{
				MessageLimit: 100,
				Burst:        1,
			},
			clk: clock.NewMock(),
		}
		limiter.Init(0)

		Convey("When the limit number of messages are reached", func() {
			clk := limiter.clk.(*clock.Mock)
			n := Nexter{
				nextCalled: make(chan *utils.Message, limiter.config.MessageLimit*2),
			}
			n.On("Next", mock.AnythingOfType("*utils.Message"))

			for i := uint64(0); i < limiter.config.MessageLimit; i++ {
				limiter.OnMessage(nil, n.Next, nil)
			}

			Convey("Then the limiter should be paused", func() {
				So(limiter.currentMessages, ShouldEqual, limiter.config.MessageLimit)
				So(limiter.paused, ShouldBeTrue)
			})

			Convey("Then after 1 second the limiter should be ready again", func() {
				clk.Add(1 * time.Second)
				limiter.OnMessage(nil, n.Next, nil)
				So(limiter.currentMessages, ShouldEqual, 1)
				So(limiter.paused, ShouldBeFalse)
			})
		})
	})

	Convey("Given an Limiter with 1000 bytes per second without burst", t, func() {
		limiter := &Limiter{
			config: Config{
				BytesLimit: 1000,
				Burst:      1,
			},
			clk: clock.NewMock(),
		}
		limiter.Init(0)

		Convey("When messages are sent", func() {
			n := Nexter{
				nextCalled: make(chan *utils.Message, 100),
			}
			n.On("Next", mock.AnythingOfType("*utils.Message"))

			Convey("Then the limiter should not be paused after 750 bytes", func() {
				for i := uint64(0); i < 3; i++ {
					m := utils.NewMessage()
					payload := make([]byte, 250)
					m.PushPayload(payload)
					limiter.OnMessage(m, n.Next, nil)
				}

				So(limiter.currentBytes, ShouldEqual, 750)
				So(limiter.paused, ShouldBeFalse)
			})

			Convey("Then the limiter should be paused after 1000 bytes", func() {
				for i := uint64(0); i < 4; i++ {
					m := utils.NewMessage()
					payload := make([]byte, 250)
					m.PushPayload(payload)
					limiter.OnMessage(m, n.Next, nil)
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
				limiter.OnMessage(m, n.Next, nil)

				So(limiter.currentBytes, ShouldEqual, 250)
				So(limiter.paused, ShouldBeFalse)
			})
		})
	})

	Convey("Given a limiter with burst", t, func() {
		limiter := &Limiter{
			config: Config{
				MessageLimit: 100,
				Burst:        2,
			},
			clk: clock.NewMock(),
		}
		limiter.Init(0)

		clk := limiter.clk.(*clock.Mock)
		clk.Add(0)
		clk.Add(2 * time.Second)

		Convey("When the limit number of messages are reached", func() {
			n := Nexter{
				nextCalled: make(chan *utils.Message, limiter.config.MessageLimit*2),
			}
			n.On("Next", mock.AnythingOfType("*utils.Message"))

			for i := uint64(0); i < limiter.config.MessageLimit; i++ {
				limiter.OnMessage(nil, n.Next, nil)
			}

			Convey("Then should be 2 burst available", func() {
				So(len(limiter.keepSending), ShouldEqual, 2)
			})
			Convey("Then messages are not blocked after the limit", func() {
				for i := uint64(0); i < limiter.config.MessageLimit; i++ {
					limiter.OnMessage(nil, n.Next, nil)
				}
				So(limiter.currentMessages, ShouldEqual, 100)
			})
			Convey("Then the limiter blocks again after reaching limit a second time", func() {
				So(limiter.paused, ShouldBeTrue)
			})
		})
	})
}
