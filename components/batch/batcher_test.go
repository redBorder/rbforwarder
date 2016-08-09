package batcher

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/oleiade/lane"
	"github.com/redBorder/rbforwarder/types"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
)

type NexterDoner struct {
	mock.Mock
	nextCalled chan *types.Message
}

func (nd *NexterDoner) Next(m *types.Message) {
	nd.Called(m)
	nd.nextCalled <- m
}

func TestBatcher(t *testing.T) {
	Convey("Given a batcher", t, func() {
		batcher := &Batcher{config: Config{
			TimeoutMillis:     1000,
			Limit:             10,
			MaxPendingBatches: 10,
		},
			clk: clock.NewMock(),
		}

		batcher.Init(0)

		Convey("When a message is received, but not yet sent", func() {
			m := &types.Message{
				Payload: lane.NewStack(),
				Opts:    lane.NewStack(),
				Reports: lane.NewStack(),
			}
			m.Payload.Push([]byte("Hello World"))
			m.Opts.Push(map[string]interface{}{
				"batch_group": "group1",
			})
			m.Reports.Push("Report")

			batcher.OnMessage(m, nil, nil)

			Convey("Message should be present on the batch", func() {
				var batch *Batch
				var exists bool
				batch, exists = batcher.batches["group1"]

				So(exists, ShouldBeTrue)
				data := batch.Message.Payload.Pop().([]byte)
				opts := batch.Message.Opts.Pop().(map[string]interface{})
				report := batch.Message.Reports.Pop().(string)
				So(string(data), ShouldEqual, "Hello World")
				So(opts["batch_group"], ShouldEqual, "group1")
				So(report, ShouldEqual, "Report")
				So(len(batcher.batches), ShouldEqual, 1)
			})
		})

		Convey("When the max number of messages is reached", func() {
			var messages []*types.Message

			for i := 0; i < int(batcher.config.Limit); i++ {
				m := &types.Message{
					Payload: lane.NewStack(),
					Opts:    lane.NewStack(),
					Reports: lane.NewStack(),
				}
				m.Payload.Push([]byte("ABC"))
				m.Opts.Push(map[string]interface{}{
					"batch_group": "group1",
				})
				m.Reports.Push("Report")

				messages = append(messages, m)
			}

			nd := new(NexterDoner)
			nd.nextCalled = make(chan *types.Message)
			nd.On("Next", mock.AnythingOfType("*types.Message")).Times(1)

			for i := 0; i < int(batcher.config.Limit); i++ {
				batcher.OnMessage(messages[i], nd.Next, nil)
			}

			Convey("The batch should be sent", func() {
				m := <-nd.nextCalled
				nd.AssertExpectations(t)
				data := m.Payload.Pop().([]byte)
				optsSize := m.Opts.Size()
				reportsSize := m.Reports.Size()
				So(string(data), ShouldEqual, "ABCABCABCABCABCABCABCABCABCABC")
				So(m.Payload.Empty(), ShouldBeTrue)
				So(optsSize, ShouldEqual, batcher.config.Limit)
				So(reportsSize, ShouldEqual, batcher.config.Limit)
				So(batcher.batches["group1"], ShouldBeNil)
				So(len(batcher.batches), ShouldEqual, 0)
			})
		})

		Convey("When the timeout expires", func() {
			var messages []*types.Message

			for i := 0; i < 5; i++ {
				m := &types.Message{
					Payload: lane.NewStack(),
					Opts:    lane.NewStack(),
					Reports: lane.NewStack(),
				}
				m.Payload.Push([]byte("ABC"))
				m.Opts.Push(map[string]interface{}{
					"batch_group": "group1",
				})
				m.Reports.Push("Report")

				messages = append(messages, m)
			}

			nd := new(NexterDoner)
			nd.nextCalled = make(chan *types.Message, 1)
			nd.On("Next", mock.AnythingOfType("*types.Message")).Times(1)

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

		Convey("When multiple messages are received with differente groups", func() {
			m1 := &types.Message{
				Payload: lane.NewStack(),
				Opts:    lane.NewStack(),
				Reports: lane.NewStack(),
			}
			m1.Payload.Push([]byte("MESSAGE 1"))
			m1.Opts.Push(map[string]interface{}{
				"batch_group": "group1",
			})

			m2 := &types.Message{
				Payload: lane.NewStack(),
				Opts:    lane.NewStack(),
				Reports: lane.NewStack(),
			}
			m2.Payload.Push([]byte("MESSAGE 2"))
			m2.Opts.Push(map[string]interface{}{
				"batch_group": "group2",
			})

			m3 := &types.Message{
				Payload: lane.NewStack(),
				Opts:    lane.NewStack(),
				Reports: lane.NewStack(),
			}
			m3.Payload.Push([]byte("MESSAGE 3"))
			m3.Opts.Push(map[string]interface{}{
				"batch_group": "group2",
			})

			nd := new(NexterDoner)
			nd.nextCalled = make(chan *types.Message, 2)
			nd.On("Next", mock.AnythingOfType("*types.Message")).Times(2)

			batcher.OnMessage(m1, nd.Next, nil)
			batcher.OnMessage(m2, nd.Next, nil)
			batcher.OnMessage(m3, nd.Next, nil)

			Convey("Each message should be in its group", func() {
				var err error
				group1 := batcher.batches["group1"].Message.Payload.Pop().([]byte)
				So(err, ShouldBeNil)

				group2 := batcher.batches["group2"].Message.Payload.Pop().([]byte)
				So(err, ShouldBeNil)

				So(string(group1), ShouldEqual, "MESSAGE 1")
				So(string(group2), ShouldEqual, "MESSAGE 2MESSAGE 3")
				So(len(batcher.batches), ShouldEqual, 2)
			})

			Convey("After a timeout the messages should be sent", func() {
				clk := batcher.clk.(*clock.Mock)
				So(len(batcher.batches), ShouldEqual, 2)
				clk.Add(time.Duration(batcher.config.TimeoutMillis) * time.Millisecond)
				group1 := <-nd.nextCalled
				group1Data := group1.Payload.Pop().([]byte)
				So(string(group1Data), ShouldEqual, "MESSAGE 1")
				group2 := <-nd.nextCalled
				group2Data := group2.Payload.Pop().([]byte)
				So(string(group2Data), ShouldEqual, "MESSAGE 2MESSAGE 3")
				So(batcher.batches["group1"], ShouldBeNil)
				So(batcher.batches["group2"], ShouldBeNil)
				So(len(batcher.batches), ShouldEqual, 0)
				nd.AssertExpectations(t)
			})
		})
	})
}
