package batcher

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/types"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
)

type NexterDoner struct {
	mock.Mock
	nextCalled chan struct{}
}

func (nd *NexterDoner) Next(m types.Messenger) {
	nd.nextCalled <- struct{}{}
	nd.Called(m)
}

func (nd *NexterDoner) Done(m types.Messenger, code int, status string) {
	nd.Called(m, code, status)
}

type TestMessage struct {
	mock.Mock
}

func (m *TestMessage) PopData() (data []byte, err error) {
	args := m.Called()

	return args.Get(0).([]byte), args.Error(1)
}

func (m *TestMessage) PopOpts() (opts map[string]interface{}, err error) {
	args := m.Called()

	return args.Get(0).(map[string]interface{}), args.Error(1)
}

func (m *TestMessage) Reports() []types.Reporter {
	return nil
}

func TestRBForwarder(t *testing.T) {
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
			m := new(TestMessage)
			m.On("PopData").Return([]byte("Hello World"), nil)
			m.On("PopOpts").Return(map[string]interface{}{
				"batch_group": "group1",
			}, nil)

			batcher.OnMessage(m, nil, nil)

			Convey("Message should be present on the batch", func() {
				var data *BatchMessage
				var exists bool
				data, exists = batcher.batches["group1"]

				So(exists, ShouldBeTrue)
				So(string(data.Buff.Bytes()), ShouldEqual, "Hello World")
				So(len(batcher.batches), ShouldEqual, 1)

				m.AssertExpectations(t)
			})
		})

		Convey("When the max number of messages is reached", func() {
			m := new(TestMessage)
			m.On("PopData").Return([]byte("ABC"), nil)
			m.On("PopOpts").Return(map[string]interface{}{
				"batch_group": "group1",
			}, nil)

			nd := new(NexterDoner)
			nd.nextCalled = make(chan struct{}, 1)
			nd.On("Next", mock.MatchedBy(func(m *BatchMessage) bool {
				data, _ := m.PopData()
				return string(data) == "ABCABCABCABCABCABCABCABCABCABC"
			})).Times(1)

			for i := 0; i < int(batcher.config.Limit); i++ {
				batcher.OnMessage(m, nd.Next, nil)
			}

			Convey("The batch should be sent", func() {
				nd.AssertExpectations(t)
				<-nd.nextCalled
				So(batcher.batches["group1"], ShouldBeNil)
				So(len(batcher.batches), ShouldEqual, 0)
			})
		})

		Convey("When the timeout expires", func() {
			m := new(TestMessage)
			m.On("PopData").Return([]byte("ABC"), nil)
			m.On("PopOpts").Return(map[string]interface{}{
				"batch_group": "group1",
			}, nil)

			nd := new(NexterDoner)
			nd.nextCalled = make(chan struct{}, 1)
			nd.On("Next", mock.MatchedBy(func(m *BatchMessage) bool {
				data, _ := m.PopData()
				return string(data) == "ABCABCABCABCABC"
			})).Times(1)

			for i := 0; i < 5; i++ {
				batcher.OnMessage(m, nd.Next, nil)
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
			m1 := new(TestMessage)
			m1.On("PopData").Return([]byte("MESSAGE 1"), nil)
			m1.On("PopOpts").Return(map[string]interface{}{
				"batch_group": "group1",
			}, nil)

			m2 := new(TestMessage)
			m2.On("PopData").Return([]byte("MESSAGE 2"), nil)
			m2.On("PopOpts").Return(map[string]interface{}{
				"batch_group": "group2",
			}, nil)

			m3 := new(TestMessage)
			m3.On("PopData").Return([]byte("MESSAGE 3"), nil)
			m3.On("PopOpts").Return(map[string]interface{}{
				"batch_group": "group2",
			}, nil)

			nd := new(NexterDoner)
			nd.nextCalled = make(chan struct{}, 2)
			nd.On("Next", mock.AnythingOfType("*batcher.BatchMessage")).Times(2)

			for i := 0; i < 3; i++ {
				batcher.OnMessage(m1, nd.Next, nil)
			}
			for i := 0; i < 3; i++ {
				batcher.OnMessage(m2, nd.Next, nil)
			}
			batcher.OnMessage(m3, nd.Next, nil)

			Convey("Each message should be in its group", func() {
				var err error
				group1, err := batcher.batches["group1"].PopData()
				So(err, ShouldBeNil)
				group2, err := batcher.batches["group2"].PopData()
				So(err, ShouldBeNil)
				So(string(group1), ShouldEqual, "MESSAGE 1MESSAGE 1MESSAGE 1")
				So(string(group2), ShouldEqual, "MESSAGE 2MESSAGE 2MESSAGE 2MESSAGE 3")
				So(len(batcher.batches), ShouldEqual, 2)
				m1.AssertExpectations(t)
				m2.AssertExpectations(t)
				m3.AssertExpectations(t)
			})

			Convey("After a timeout the messages should be sent", func() {
				clk := batcher.clk.(*clock.Mock)
				So(len(batcher.batches), ShouldEqual, 2)
				clk.Add(1000 * time.Second)
				<-nd.nextCalled
				<-nd.nextCalled
				So(batcher.batches["group1"], ShouldBeNil)
				So(batcher.batches["group2"], ShouldBeNil)
				So(len(batcher.batches), ShouldEqual, 0)
				nd.AssertExpectations(t)
			})
		})
	})
}
