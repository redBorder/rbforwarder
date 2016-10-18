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

package utils

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMessage(t *testing.T) {
	Convey("Given a message", t, func() {

		Convey("When fields are accessed", func() {
			m := NewMessage()

			Convey("Then fields has to be initialized", func() {
				So(m.Opts, ShouldNotBeNil)
				So(m.payload, ShouldNotBeNil)
				So(m.Reports, ShouldNotBeNil)
			})
		})
	})

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
