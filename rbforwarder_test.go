package rbforwarder

import (
	"testing"

	"github.com/redBorder/rbforwarder/types"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
)

type MockMiddleComponent struct {
	mock.Mock
}

func (c *MockMiddleComponent) Init(id int) error {
	args := c.Called()
	return args.Error(0)
}

func (c *MockMiddleComponent) OnMessage(
	m types.Messenger,
	next types.Next,
	done types.Done,
) {
	c.Called(m)
	data, _ := m.PopData()
	processedData := "-> [" + string(data) + "] <-"
	m.PushData([]byte(processedData))
	next(m)

}

type MockComponent struct {
	mock.Mock

	channel chan string

	status     string
	statusCode int
}

func (c *MockComponent) Init(id int) error {
	args := c.Called()
	return args.Error(0)
}

func (c *MockComponent) OnMessage(
	m types.Messenger,
	next types.Next,
	done types.Done,
) {
	c.Called(m)

	data, _ := m.PopData()
	c.channel <- string(data)
	done(m, c.statusCode, c.status)
}

func TestRBForwarder(t *testing.T) {
	Convey("Given a single component working pipeline", t, func() {
		numMessages := 10000
		numWorkers := 10
		numRetries := 3

		component := &MockComponent{
			channel: make(chan string, 10000),
		}

		rbforwarder := NewRBForwarder(Config{
			Retries:   numRetries,
			QueueSize: numMessages,
		})

		component.On("Init").Return(nil).Times(numWorkers)

		var components []types.Composer
		var instances []int
		components = append(components, component)
		instances = append(instances, numWorkers)

		rbforwarder.PushComponents(components, instances)

		////////////////////////////////////////////////////////////////////////////

		Convey("When a \"Hello World\" message is produced", func() {
			component.status = "OK"
			component.statusCode = 0

			component.On("OnMessage", mock.AnythingOfType("*rbforwarder.message")).Times(1)

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
			)

			Convey("\"Hello World\" message should be get by the worker", func() {
				var lastReport Report
				var reports int
				for report := range rbforwarder.GetReports() {
					reports++
					lastReport = report
					rbforwarder.Close()
				}

				So(lastReport, ShouldNotBeNil)
				So(reports, ShouldEqual, 1)
				So(lastReport.opts["message_id"], ShouldEqual, "test123")
				So(lastReport.code, ShouldEqual, 0)
				So(lastReport.status, ShouldEqual, "OK")
				So(err, ShouldBeNil)

				component.AssertExpectations(t)
			})
		})

		// ////////////////////////////////////////////////////////////////////////////

		Convey("When a message is produced after close forwarder", func() {
			rbforwarder.Close()

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
			)

			Convey("Should error", func() {
				So(err.Error(), ShouldEqual, "Forwarder has been closed")
			})
		})

		////////////////////////////////////////////////////////////////////////////

		Convey("When calling OnMessage() with options", func() {
			component.On("OnMessage", mock.AnythingOfType("*rbforwarder.message"))

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"option": "example_option"},
			)

			rbforwarder.Close()

			Convey("Should be possible to read an option", func() {
				for report := range rbforwarder.GetReports() {
					So(report.opts, ShouldNotBeNil)
					So(report.opts["option"], ShouldEqual, "example_option")
				}

				So(err, ShouldBeNil)
				component.AssertExpectations(t)
			})

			Convey("Should not be possible to read an nonexistent option", func() {
				for report := range rbforwarder.GetReports() {
					So(report.opts, ShouldNotBeNil)
					So(report.opts["invalid"], ShouldBeEmpty)
				}

				So(err, ShouldBeNil)
				component.AssertExpectations(t)
			})
		})

		// ////////////////////////////////////////////////////////////////////////////

		Convey("When calling OnMessage() without options", func() {
			component.On("OnMessage", mock.AnythingOfType("*rbforwarder.message"))

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				nil,
			)

			rbforwarder.Close()

			Convey("Should not be possible to read the option", func() {
				for report := range rbforwarder.GetReports() {
					So(err, ShouldBeNil)
					So(report.opts, ShouldBeNil)
				}

				So(err, ShouldBeNil)
				component.AssertExpectations(t)
			})
		})

		// ////////////////////////////////////////////////////////////////////////////

		Convey("When a message fails to send", func() {
			component.status = "Fake Error"
			component.statusCode = 99

			component.On("OnMessage", mock.AnythingOfType("*rbforwarder.message")).Times(4)

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
			)

			Convey("The message should be retried", func() {
				So(err, ShouldBeNil)

				var reports int
				var lastReport Report
				for report := range rbforwarder.GetReports() {
					reports++
					lastReport = report
					rbforwarder.Close()
				}

				So(lastReport, ShouldNotBeNil)
				So(reports, ShouldEqual, 1)
				So(lastReport.opts["message_id"], ShouldEqual, "test123")
				So(lastReport.status, ShouldEqual, "Fake Error")
				So(lastReport.code, ShouldEqual, 99)
				So(lastReport.retries, ShouldEqual, numRetries)

				component.AssertExpectations(t)
			})
		})

		////////////////////////////////////////////////////////////////////////////

		Convey("When 10000 messages are produced", func() {
			var numErr int

			component.On("OnMessage", mock.AnythingOfType("*rbforwarder.message")).
				Return(nil).
				Times(numMessages)

			for i := 0; i < numMessages; i++ {
				if err := rbforwarder.Produce([]byte("Hello World"),
					map[string]interface{}{"message_id": i},
				); err != nil {
					numErr++
				}
			}

			Convey("10000 reports should be received", func() {
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

			Convey("10000 reports should be received in order", func() {
				ordered := true
				var reports int

				for report := range rbforwarder.GetOrderedReports() {
					if report.opts["message_id"] != reports {
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
			component1.On("Init").Return(nil)
			component2.On("Init").Return(nil)
		}

		var components []types.Composer
		var instances []int

		components = append(components, component1)
		components = append(components, component2)

		instances = append(instances, numWorkers)
		instances = append(instances, numWorkers)

		rbforwarder.PushComponents(components, instances)

		Convey("When a \"Hello World\" message is produced", func() {
			component2.status = "OK"
			component2.statusCode = 0

			component1.On("OnMessage", mock.AnythingOfType("*rbforwarder.message"))
			component2.On("OnMessage", mock.AnythingOfType("*rbforwarder.message"))

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
			)

			rbforwarder.Close()

			Convey("\"Hello World\" message should be processed by the pipeline", func() {
				reports := 0
				for report := range rbforwarder.GetReports() {
					reports++

					So(report.opts["message_id"], ShouldEqual, "test123")
					So(report.code, ShouldEqual, 0)
					So(report.status, ShouldEqual, "OK")
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
