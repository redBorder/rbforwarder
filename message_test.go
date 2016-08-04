package rbforwarder

import (
	"testing"

	"github.com/oleiade/lane"
	. "github.com/smartystreets/goconvey/convey"
)

func TestMessage(t *testing.T) {
	Convey("Given a payload and some metadata", t, func() {
		payload := "This is the payload"
		metadata := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		}

		Convey("When the data is stored in a message", func() {
			m := new(message)
			m.payload = lane.NewStack()
			m.opts = lane.NewStack()

			m.payload.Push([]byte(payload))
			m.opts.Push(metadata)

			Convey("Then the data can be recovered through messenger methods", func() {
				data, err := m.PopData()
				So(err, ShouldBeNil)

				opts, err := m.PopOpts()
				So(err, ShouldBeNil)

				So(string(data), ShouldEqual, payload)
				So(opts, ShouldNotBeNil)

				So(opts["key1"], ShouldEqual, "value1")
				So(opts["key2"], ShouldEqual, "value2")
			})
		})
	})

	Convey("Given a message with nil data and opts", t, func() {
		m := new(message)

		Convey("When trying to get message data", func() {
			data, err1 := m.PopData()
			opts, err2 := m.PopOpts()

			Convey("Then should error", func() {
				So(err1, ShouldNotBeNil)
				So(err2, ShouldNotBeNil)
				So(err1.Error(), ShouldEqual, "Uninitialized payload")
				So(err2.Error(), ShouldEqual, "Uninitialized options")
				So(data, ShouldBeNil)
				So(opts, ShouldBeNil)
			})
		})
	})

	Convey("Given a message with no options or no data", t, func() {
		m := &message{
			payload: lane.NewStack(),
			opts:    lane.NewStack(),
		}

		Convey("When trying to get message data", func() {
			data, err1 := m.PopData()
			opts, err2 := m.PopOpts()

			Convey("Then should error", func() {
				So(err1, ShouldNotBeNil)
				So(err2, ShouldNotBeNil)
				So(err1.Error(), ShouldEqual, "No data")
				So(err2.Error(), ShouldEqual, "No options")
				So(data, ShouldBeNil)
				So(opts, ShouldBeNil)
			})
		})
	})

	Convey("Given a delivered message", t, func() {
		m := &message{
			payload: lane.NewStack(),
			opts:    lane.NewStack(),
			code:    0,
			status:  "testing",
			retries: 10,
		}

		m.payload.Push([]byte("This message is delivered"))
		m.opts.Push(map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		})

		Convey("When the user needs a report for the message", func() {
			r := m.Reports()

			Convey("Then a report can be obtained for the message", func() {
				So(len(r), ShouldEqual, 1)

				code, status, retries := r[0].Status()
				So(code, ShouldEqual, 0)
				So(status, ShouldEqual, "testing")
				So(retries, ShouldEqual, 10)

				opts := r[0].GetOpts()
				So(opts["key1"], ShouldEqual, "value1")
				So(opts["key2"], ShouldEqual, "value2")
			})
		})
	})
}
