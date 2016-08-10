package types

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMessage(t *testing.T) {
	Convey("Given a payload", t, func() {
		payload := "This is the payload"

		Convey("When the data is stored in a message", func() {
			m := NewMessage()
			m.PushPayload([]byte(payload))

			Convey("Then the data can be recovered through messenger methods", func() {
				data, err := m.PopPayload()
				So(err, ShouldBeNil)
				So(string(data), ShouldEqual, payload)
			})
		})
	})

	Convey("Given a message with no options or no data", t, func() {
		m := NewMessage()

		Convey("When trying to get message data", func() {
			_, err := m.PopPayload()

			Convey("Then should error", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "No payload available")
			})
		})
	})
}
