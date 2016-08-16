package batcher

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/utils"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
)

type NexterDoner struct {
	mock.Mock
	nextCalled chan *utils.Message
}

func (nd *NexterDoner) Next(m *utils.Message) {
	nd.Called(m)
	nd.nextCalled <- m
}

func TestBatcher(t *testing.T) {
	Convey("Given a batcher", t, func() {
		batcher := &Batcher{
			Config: Config{
				TimeoutMillis:     1000,
				Limit:             10,
				MaxPendingBatches: 10,
			},
		}

		batcher.Init(0)
		batcher.clk = clock.NewMock()

		Convey("When a message is received with no batch group", func() {
			m := utils.NewMessage()
			m.PushPayload([]byte("Hello World"))

			nd := new(NexterDoner)
			nd.nextCalled = make(chan *utils.Message, 1)
			nd.On("Next", mock.AnythingOfType("*utils.Message")).Times(1)

			batcher.OnMessage(m, nd.Next, nil)

			Convey("Message should be present on the batch", func() {
				nd.AssertExpectations(t)
				m := <-nd.nextCalled
				So(len(batcher.batches), ShouldEqual, 0)
				payload, err := m.PopPayload()
				So(err, ShouldBeNil)
				So(string(payload), ShouldEqual, "Hello World")
			})
		})

		Convey("When a message is received, but not yet sent", func() {
			m := utils.NewMessage()
			m.PushPayload([]byte("Hello World"))
			m.Opts = map[string]interface{}{
				"batch_group": "group1",
			}
			m.Reports.Push("Report")

			batcher.OnMessage(m, nil, nil)

			Convey("Message should be present on the batch", func() {
				batch, exists := batcher.batches["group1"]
				So(exists, ShouldBeTrue)

				data := batch.Buff.Bytes()
				So(string(data), ShouldEqual, "Hello World")

				opts := batch.Message.Opts
				So(opts["batch_group"], ShouldEqual, "group1")

				report := batch.Message.Reports.Pop().(string)
				So(report, ShouldEqual, "Report")

				So(len(batcher.batches), ShouldEqual, 1)
			})
		})

		Convey("When the max number of messages is reached", func() {
			var messages []*utils.Message

			for i := 0; i < int(batcher.Config.Limit); i++ {
				m := utils.NewMessage()
				m.PushPayload([]byte("ABC"))
				m.Opts = map[string]interface{}{
					"batch_group": "group1",
				}
				m.Reports.Push("Report")

				messages = append(messages, m)
			}

			nd := new(NexterDoner)
			nd.nextCalled = make(chan *utils.Message, 1)
			nd.On("Next", mock.AnythingOfType("*utils.Message")).Times(1)

			for i := 0; i < int(batcher.Config.Limit); i++ {
				batcher.OnMessage(messages[i], nd.Next, nil)
			}

			Convey("The batch should be sent", func() {
				m := <-nd.nextCalled
				nd.AssertExpectations(t)
				data, err := m.PopPayload()

				So(err, ShouldBeNil)
				So(string(data), ShouldEqual, "ABCABCABCABCABCABCABCABCABCABC")
				So(m.Reports.Size(), ShouldEqual, batcher.Config.Limit)
				So(batcher.batches["group1"], ShouldBeNil)
				So(len(batcher.batches), ShouldEqual, 0)
			})
		})

		Convey("When the timeout expires", func() {
			var messages []*utils.Message

			for i := 0; i < 5; i++ {
				m := utils.NewMessage()
				m.PushPayload([]byte("Hello World"))
				m.Opts = map[string]interface{}{
					"batch_group": "group1",
				}
				m.Reports.Push("Report")

				messages = append(messages, m)
			}

			nd := new(NexterDoner)
			nd.nextCalled = make(chan *utils.Message, 1)
			nd.On("Next", mock.AnythingOfType("*utils.Message")).Times(1)

			for i := 0; i < 5; i++ {
				batcher.OnMessage(messages[i], nd.Next, nil)
			}

			clk := batcher.clk.(*clock.Mock)

			Convey("The batch should be sent", func() {
				clk.Add(500 * time.Millisecond)
				So(batcher.batches["group1"], ShouldNotBeNil)
				clk.Add(500 * time.Millisecond)
				<-nd.nextCalled
				So(batcher.batches["group1"], ShouldBeNil)
				So(len(batcher.batches), ShouldEqual, 0)
				nd.AssertExpectations(t)
			})
		})

		Convey("When multiple messages are received with differents groups", func() {
			m1 := utils.NewMessage()
			m1.PushPayload([]byte("MESSAGE 1"))
			m1.Reports.Push("Report 1")
			m1.Opts = map[string]interface{}{
				"batch_group": "group1",
			}

			m2 := utils.NewMessage()
			m2.PushPayload([]byte("MESSAGE 2"))
			m2.Reports.Push("Report 2")
			m2.Opts = map[string]interface{}{
				"batch_group": "group2",
			}
			m3 := utils.NewMessage()
			m3.PushPayload([]byte("MESSAGE 3"))
			m3.Reports.Push("Report 3")
			m3.Opts = map[string]interface{}{
				"batch_group": "group2",
			}

			nd := new(NexterDoner)
			nd.nextCalled = make(chan *utils.Message, 2)
			nd.On("Next", mock.AnythingOfType("*utils.Message")).Times(2)

			batcher.OnMessage(m1, nd.Next, nil)
			batcher.OnMessage(m2, nd.Next, nil)
			batcher.OnMessage(m3, nd.Next, nil)

			Convey("Each message should be in its group", func() {
				group1 := batcher.batches["group1"].Buff.Bytes()
				So(string(group1), ShouldEqual, "MESSAGE 1")

				group2 := batcher.batches["group2"].Buff.Bytes()
				So(string(group2), ShouldEqual, "MESSAGE 2MESSAGE 3")

				So(len(batcher.batches), ShouldEqual, 2)
			})

			Convey("After a timeout the messages should be sent", func() {
				clk := batcher.clk.(*clock.Mock)
				So(len(batcher.batches), ShouldEqual, 2)

				clk.Add(time.Duration(batcher.Config.TimeoutMillis) * time.Millisecond)

				group1 := <-nd.nextCalled
				group1Data, err := group1.PopPayload()
				report1 := group1.Reports.Pop().(string)
				So(err, ShouldBeNil)
				So(report1, ShouldEqual, "Report 1")

				group2 := <-nd.nextCalled
				group2Data, err := group2.PopPayload()
				So(err, ShouldBeNil)
				report3 := group2.Reports.Pop().(string)
				So(report3, ShouldEqual, "Report 3")
				report2 := group2.Reports.Pop().(string)
				So(report2, ShouldEqual, "Report 2")

				So(string(group1Data), ShouldEqual, "MESSAGE 1")
				So(string(group2Data), ShouldEqual, "MESSAGE 2MESSAGE 3")
				So(batcher.batches["group1"], ShouldBeNil)
				So(batcher.batches["group2"], ShouldBeNil)
				So(len(batcher.batches), ShouldEqual, 0)

				nd.AssertExpectations(t)
			})
		})
	})
}
