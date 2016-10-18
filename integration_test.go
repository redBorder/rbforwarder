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

// +build integration

package rbforwarder

import (
	"testing"

	"github.com/redBorder/rbforwarder/utils"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
)

type MockMiddleComponent struct {
	mock.Mock
}

func (c *MockMiddleComponent) Workers() int {
	args := c.Called()
	return args.Int(0)
}

func (c *MockMiddleComponent) Spawn(id int) utils.Composer {
	args := c.Called()
	return args.Get(0).(utils.Composer)
}

func (c *MockMiddleComponent) OnMessage(m *utils.Message, done utils.Done) {
	c.Called(m)
	if data, err := m.PopPayload(); err == nil {
		processedData := "-> [" + string(data) + "] <-"
		m.PushPayload([]byte(processedData))
	}

	done(m, 0, "")
}

type MockComponent struct {
	mock.Mock

	channel chan string

	status     string
	statusCode int
}

func (c *MockComponent) Workers() int {
	args := c.Called()
	return args.Int(0)
}

func (c *MockComponent) Spawn(id int) utils.Composer {
	args := c.Called()
	return args.Get(0).(utils.Composer)
}

func (c *MockComponent) OnMessage(m *utils.Message, done utils.Done) {
	c.Called(m)
	if data, err := m.PopPayload(); err == nil {
		c.channel <- string(data)
	} else {
		c.channel <- err.Error()
	}

	done(m, c.statusCode, c.status)
}

func TestRBForwarder(t *testing.T) {
	Convey("Given a single component working pipeline", t, func() {
		numMessages := 1000
		numWorkers := 100
		numRetries := 3

		component := &MockComponent{
			channel: make(chan string, 10000),
		}

		rbforwarder := NewRBForwarder(Config{
			Retries:   numRetries,
			QueueSize: numMessages,
		})

		component.On("Workers").Return(numWorkers)
		component.On("Spawn").Return(component).Times(numWorkers)

		var components []interface{}
		components = append(components, component)

		rbforwarder.PushComponents(components)
		rbforwarder.Run()

		////////////////////////////////////////////////////////////////////////////

		Convey("When a \"Hello World\" message is produced", func() {
			component.status = "OK"
			component.statusCode = 0

			component.On("OnMessage", mock.AnythingOfType("*utils.Message"))

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
				nil,
			)

			Convey("Then \"Hello World\" message should be get by the worker", func() {
				var lastReport Report
				var reports int
				for r := range rbforwarder.GetReports() {
					reports++
					lastReport = r.(Report)
					rbforwarder.Close()
				}

				So(lastReport, ShouldNotBeNil)
				So(reports, ShouldEqual, 1)
				So(lastReport.Code, ShouldEqual, 0)
				So(lastReport.Status, ShouldEqual, "OK")
				So(err, ShouldBeNil)

				component.AssertExpectations(t)
			})
		})

		////////////////////////////////////////////////////////////////////////////

		Convey("When a message is produced after close forwarder", func() {
			rbforwarder.Close()

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
				nil,
			)

			Convey("Should error", func() {
				So(err.Error(), ShouldEqual, "Forwarder has been closed")
			})
		})

		////////////////////////////////////////////////////////////////////////////

		Convey("When calling OnMessage() with opaque", func() {
			component.On("OnMessage", mock.AnythingOfType("*utils.Message"))

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				nil,
				"This is an opaque",
			)

			Convey("Should be possible to read the opaque", func() {
				So(err, ShouldBeNil)

				var reports int
				var lastReport Report
				for r := range rbforwarder.GetReports() {
					reports++
					lastReport = r.(Report)
					rbforwarder.Close()
				}

				opaque := lastReport.Opaque.(string)
				So(opaque, ShouldEqual, "This is an opaque")
			})
		})

		////////////////////////////////////////////////////////////////////////////

		Convey("When a message fails to send", func() {
			component.status = "Fake Error"
			component.statusCode = 99

			component.On("OnMessage", mock.AnythingOfType("*utils.Message")).Times(4)

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
				nil,
			)

			Convey("The message should be retried", func() {
				So(err, ShouldBeNil)

				var reports int
				var lastReport Report
				for r := range rbforwarder.GetReports() {
					reports++
					lastReport = r.(Report)
					rbforwarder.Close()
				}

				So(lastReport, ShouldNotBeNil)
				So(reports, ShouldEqual, 1)
				So(lastReport.Status, ShouldEqual, "Fake Error")
				So(lastReport.Code, ShouldEqual, 99)
				So(lastReport.retries, ShouldEqual, numRetries)

				component.AssertExpectations(t)
			})
		})

		////////////////////////////////////////////////////////////////////////////

		Convey("When multiple messages are produced", func() {
			var numErr int

			component.On("OnMessage", mock.AnythingOfType("*utils.Message")).
				Return(nil).
				Times(numMessages)

			for i := 0; i < numMessages; i++ {
				if err := rbforwarder.Produce([]byte("Hello World"),
					nil,
					i,
				); err != nil {
					numErr++
				}
			}

			Convey("Then reports should be received", func() {
				var reports int
				for range rbforwarder.GetReports() {
					reports++
					if reports >= numMessages {
						rbforwarder.Close()
					}
				}

				So(numErr, ShouldBeZeroValue)
				So(reports, ShouldEqual, numMessages)

				component.AssertExpectations(t)
			})

			Convey("Then reports should be received in order", func() {
				ordered := true
				var reports int

				for rep := range rbforwarder.GetOrderedReports() {
					if rep.(Report).Opaque.(int) != reports {
						ordered = false
					}
					reports++
					if reports >= numMessages {
						rbforwarder.Close()
					}
				}

				So(numErr, ShouldBeZeroValue)
				So(ordered, ShouldBeTrue)
				So(reports, ShouldEqual, numMessages)

				component.AssertExpectations(t)
			})
		})
	})

	Convey("Given a multi-component working pipeline", t, func() {
		numMessages := 100
		numWorkers := 3
		numRetries := 3

		component1 := &MockMiddleComponent{}
		component2 := &MockComponent{
			channel: make(chan string, 10000),
		}

		rbforwarder := NewRBForwarder(Config{
			Retries:   numRetries,
			QueueSize: numMessages,
		})

		for i := 0; i < numWorkers; i++ {
			component1.On("Spawn").Return(component1)
			component2.On("Spawn").Return(component2)
			component1.On("Workers").Return(numWorkers)
			component2.On("Workers").Return(numWorkers)
		}

		var components []interface{}

		components = append(components, component1)
		components = append(components, component2)

		rbforwarder.PushComponents(components)
		rbforwarder.Run()

		Convey("When a \"Hello World\" message is produced", func() {
			component2.status = "OK"
			component2.statusCode = 0

			component1.On("OnMessage", mock.AnythingOfType("*utils.Message"))
			component2.On("OnMessage", mock.AnythingOfType("*utils.Message"))

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
				nil,
			)

			rbforwarder.Close()

			Convey("\"Hello World\" message should be processed by the pipeline", func() {
				reports := 0
				for rep := range rbforwarder.GetReports() {
					reports++

					code := rep.(Report).Code
					status := rep.(Report).Status
					So(code, ShouldEqual, 0)
					So(status, ShouldEqual, "OK")
				}

				m := <-component2.channel

				So(err, ShouldBeNil)
				So(reports, ShouldEqual, 1)
				So(m, ShouldEqual, "-> [Hello World] <-")

				component1.AssertExpectations(t)
				component2.AssertExpectations(t)
			})
		})
	})
}
