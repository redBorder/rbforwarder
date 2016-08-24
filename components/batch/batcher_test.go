package batcher

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/redBorder/rbforwarder/utils"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/streamrail/concurrent-map"
	"github.com/stretchr/testify/mock"
)

type Doner struct {
	mock.Mock
	doneCalled chan *utils.Message
}

func (d *Doner) Done(m *utils.Message, code int, status string) {
	d.Called(m, code, status)
	d.doneCalled <- m
}

func TestBatcher(t *testing.T) {
	Convey("Given a batcher", t, func() {
		batcher := &Batcher{
			Config: Config{
				Workers:           1,
				TimeoutMillis:     1000,
				Limit:             10,
				MaxPendingBatches: 10,
				Deflate:           false,
			},
		}

		b := batcher.Spawn(0).(*Batcher)
		b.clk = clock.NewMock()

		Convey("When the number of workers is requested", func() {
			workers := batcher.Workers()

			Convey("Then the number of workers should be correct", func() {
				So(workers, ShouldEqual, 1)
			})
		})

		Convey("When a message is received with no batch group", func() {
			m := utils.NewMessage()
			m.PushPayload([]byte("Hello World"))

			d := new(Doner)
			d.doneCalled = make(chan *utils.Message, 1)
			d.On("Done", mock.AnythingOfType("*utils.Message"), 0, "").Times(1)

			b.OnMessage(m, d.Done)
			result := <-d.doneCalled

			Convey("Then the message should be sent as is", func() {
				So(b.batches.Count(), ShouldEqual, 0)
				payload, err := result.PopPayload()
				So(err, ShouldBeNil)
				So(string(payload), ShouldEqual, "Hello World")

				d.AssertExpectations(t)
			})
		})

		Convey("When a message is received by the worker, but not yet sent", func() {
			m := utils.NewMessage()
			m.PushPayload([]byte("Hello World"))
			m.Opts = cmap.New()
			m.Opts.Set("batch_group", "group1")

			b.OnMessage(m, nil)

			Convey("Message should be present on the batch", func() {
				tmp, exists := b.batches.Get("group1")
				So(exists, ShouldBeTrue)
				batch := tmp.(*Batch)

				batch.Writer.(*bufio.Writer).Flush()

				data := batch.Buf.Bytes()
				So(string(data), ShouldEqual, "Hello World")

				opts := batch.Message.Opts
				group, ok := opts.Get("batch_group")
				So(ok, ShouldBeTrue)
				So(group.(string), ShouldEqual, "group1")

				So(b.batches.Count(), ShouldEqual, 1)
			})
		})

		Convey("When the max number of messages is reached", func() {
			var messages []*utils.Message

			for i := 0; i < int(b.Config.Limit); i++ {
				m := utils.NewMessage()
				m.PushPayload([]byte("ABC"))
				m.Opts = cmap.New()
				m.Opts.Set("batch_group", "group1")

				messages = append(messages, m)
			}

			d := new(Doner)
			d.doneCalled = make(chan *utils.Message, 1)
			d.On("Done", mock.AnythingOfType("*utils.Message"), 0, "limit").Times(1)

			for i := 0; i < int(b.Config.Limit); i++ {
				b.OnMessage(messages[i], d.Done)
			}

			Convey("Then the batch should be sent", func() {
				m := <-d.doneCalled
				data, err := m.PopPayload()

				So(err, ShouldBeNil)
				So(string(data), ShouldEqual, "ABCABCABCABCABCABCABCABCABCABC")
				group1, _ := b.batches.Get("group1")
				So(group1, ShouldBeNil)
				So(b.batches.Count(), ShouldEqual, 0)

				d.AssertExpectations(t)
			})
		})

		Convey("When the timeout expires", func() {
			var messages []*utils.Message

			for i := 0; i < 5; i++ {
				m := utils.NewMessage()
				m.PushPayload([]byte("Hello World"))
				m.Opts = cmap.New()
				m.Opts.Set("batch_group", "group1")

				messages = append(messages, m)
			}

			d := new(Doner)
			d.doneCalled = make(chan *utils.Message, 1)
			d.On("Done", mock.AnythingOfType("*utils.Message"), 0, "timeout").Times(1)

			for i := 0; i < 5; i++ {
				b.OnMessage(messages[i], d.Done)
			}

			clk := b.clk.(*clock.Mock)

			Convey("The batch should be sent", func() {
				clk.Add(500 * time.Millisecond)

				group1, _ := b.batches.Get("group1")
				So(group1.(*Batch), ShouldNotBeNil)

				clk.Add(500 * time.Millisecond)

				<-d.doneCalled
				group1, _ = b.batches.Get("group1")
				So(group1, ShouldBeNil)
				So(b.batches.Count(), ShouldEqual, 0)

				d.AssertExpectations(t)
			})
		})

		Convey("When multiple messages are received with differents groups", func() {
			m1 := utils.NewMessage()
			m1.PushPayload([]byte("MESSAGE 1"))
			m1.Reports.Push("Report 1")
			m1.Opts = cmap.New()
			m1.Opts.Set("batch_group", "group1")

			m2 := utils.NewMessage()
			m2.PushPayload([]byte("MESSAGE 2"))
			m2.Reports.Push("Report 2")
			m2.Opts = cmap.New()
			m2.Opts.Set("batch_group", "group2")

			m3 := utils.NewMessage()
			m3.PushPayload([]byte("MESSAGE 3"))
			m3.Reports.Push("Report 3")
			m3.Opts = cmap.New()
			m3.Opts.Set("batch_group", "group2")

			d := new(Doner)
			d.doneCalled = make(chan *utils.Message, 2)
			d.On("Done", mock.AnythingOfType("*utils.Message"), 0, "timeout").Times(2)

			b.OnMessage(m1, d.Done)
			b.OnMessage(m2, d.Done)
			b.OnMessage(m3, d.Done)

			Convey("Then each message should be in its group", func() {
				tmp, _ := b.batches.Get("group1")
				batch := tmp.(*Batch)
				batch.Writer.(*bufio.Writer).Flush()
				group1 := batch.Buf.Bytes()
				So(string(group1), ShouldEqual, "MESSAGE 1")

				tmp, _ = b.batches.Get("group2")
				batch = tmp.(*Batch)
				batch.Writer.(*bufio.Writer).Flush()
				group2 := batch.Buf.Bytes()
				So(string(group2), ShouldEqual, "MESSAGE 2MESSAGE 3")

				So(b.batches.Count(), ShouldEqual, 2)
			})

			Convey("Then after a timeout the messages should be sent", func() {
				clk := b.clk.(*clock.Mock)
				So(b.batches.Count(), ShouldEqual, 2)

				clk.Add(time.Duration(b.Config.TimeoutMillis) * time.Millisecond)

				group1 := <-d.doneCalled
				group1Data, err := group1.PopPayload()
				report1 := group1.Reports.Pop().(string)
				So(err, ShouldBeNil)
				So(report1, ShouldEqual, "Report 1")

				group2 := <-d.doneCalled
				group2Data, err := group2.PopPayload()
				So(err, ShouldBeNil)
				report3 := group2.Reports.Pop().(string)
				So(report3, ShouldEqual, "Report 3")
				report2 := group2.Reports.Pop().(string)
				So(report2, ShouldEqual, "Report 2")

				So(string(group1Data), ShouldEqual, "MESSAGE 1")
				So(string(group2Data), ShouldEqual, "MESSAGE 2MESSAGE 3")
				batch, _ := b.batches.Get("group1")
				So(batch, ShouldBeNil)

				batch, _ = b.batches.Get("group2")
				So(batch, ShouldBeNil)
				So(b.batches.Count(), ShouldEqual, 0)

				d.AssertExpectations(t)
			})
		})
	})

	Convey("Given a batcher with compression", t, func() {
		batcher := &Batcher{
			Config: Config{
				TimeoutMillis:     1000,
				Limit:             10,
				MaxPendingBatches: 10,
				Deflate:           true,
			},
		}

		b := batcher.Spawn(0).(*Batcher)
		b.clk = clock.NewMock()

		Convey("When the max number of messages is reached", func() {
			var messages []*utils.Message

			for i := 0; i < int(b.Config.Limit); i++ {
				m := utils.NewMessage()
				m.PushPayload([]byte("ABC"))
				m.Opts = cmap.New()
				m.Opts.Set("batch_group", "group1")
				m.Reports.Push("Report")

				messages = append(messages, m)
			}

			d := new(Doner)
			d.doneCalled = make(chan *utils.Message, 1)
			d.On("Done", mock.AnythingOfType("*utils.Message"), 0, "limit").Times(1)

			for i := 0; i < int(b.Config.Limit); i++ {
				b.OnMessage(messages[i], d.Done)
			}

			Convey("The batch should be sent compressed", func() {
				m := <-d.doneCalled
				decompressed := make([]byte, 30)
				data, err := m.PopPayload()
				buf := bytes.NewBuffer(data)

				r, err := zlib.NewReader(buf)
				r.Read(decompressed)
				r.Close()

				So(err, ShouldBeNil)
				So(string(decompressed), ShouldEqual, "ABCABCABCABCABCABCABCABCABCABC")
				So(m.Reports.Size(), ShouldEqual, b.Config.Limit)
				batch, _ := b.batches.Get("group1")
				So(batch, ShouldBeNil)
				So(b.batches.Count(), ShouldEqual, 0)

				d.AssertExpectations(t)
			})
		})
	})
}
