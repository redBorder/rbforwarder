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
	args := c.Called(id)
	return args.Error(0)
}

func (c *MockMiddleComponent) OnMessage(
	m types.Messenger,
	next types.Next,
	done types.Done,
) {
	data, _ := m.PopData()
	processedData := "-> [" + string(data) + "] <-"
	m.PushData([]byte(processedData))
	next(m)

	c.Called(m)
}

type MockComponent struct {
	mock.Mock

	channel chan string

	status     string
	statusCode int
}

func (c *MockComponent) Init(id int) error {
	args := c.Called(id)
	return args.Error(0)
}

func (c *MockComponent) OnMessage(
	m types.Messenger,
	next types.Next,
	done types.Done,
) {

	data, _ := m.PopData()
	c.channel <- string(data)
	done(m, c.statusCode, c.status)
	c.Called(m)
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

		for i := 0; i < numWorkers; i++ {
			component.On("Init", i).Return(nil)
		}

		var components []types.Composer
		var instances []int
		components = append(components, component)
		instances = append(instances, numWorkers)
		rbforwarder.PushComponents(components, instances)

		Convey("When a \"Hello World\" message is produced", func() {
			component.status = "OK"
			component.statusCode = 0

			component.On("OnMessage", mock.MatchedBy(func(m *message) bool {
				opt, err := m.GetOpt("message_id")
				if err != nil {
					return false
				}

				return opt.(string) == "test123"
			}))

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
			)

			Convey("\"Hello World\" message should be get by the worker", func() {
				report := <-rbforwarder.GetReports()

				So(err, ShouldBeNil)
				So(report.opts["message_id"], ShouldEqual, "test123")
				So(report.code, ShouldEqual, 0)
				So(report.status, ShouldEqual, "OK")

				component.AssertExpectations(t)
			})
		})

		Convey("When calling OnMessage() with options", func() {
			component.On("OnMessage", mock.MatchedBy(func(m *message) bool {
				_, err := m.GetOpt("nonexistent")
				return err != nil
			}))

			Convey("Should not be possible to read an nonexistent option", func() {
				err := rbforwarder.Produce(
					[]byte("Hello World"),
					map[string]interface{}{},
				)

				report := <-rbforwarder.GetReports()

				So(err, ShouldBeNil)
				So(report.opts, ShouldNotBeNil)
				So(report.opts, ShouldBeEmpty)

				component.AssertExpectations(t)
			})
		})

		Convey("When calling OnMessage() without options", func() {
			component.On("OnMessage", mock.MatchedBy(func(m *message) bool {
				_, err := m.GetOpt("nonexistent")
				return err != nil
			}))

			Convey("Should not be possible to read the option", func() {
				err := rbforwarder.Produce(
					[]byte("Hello World"),
					nil,
				)

				report := <-rbforwarder.GetReports()

				So(err, ShouldBeNil)
				So(report.opts, ShouldBeNil)

				component.AssertExpectations(t)
			})
		})

		Convey("When a message fails to send", func() {
			component.status = "Fake Error"
			component.statusCode = 99

			component.On("OnMessage", mock.MatchedBy(func(m *message) bool {
				opt, err := m.GetOpt("message_id")
				if err != nil {
					return false
				}

				return opt.(string) == "test123"
			}))

			err := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
			)

			Convey("The message should be retried\n", func() {
				report := <-rbforwarder.GetReports()

				So(err, ShouldBeNil)
				So(report.opts["message_id"], ShouldEqual, "test123")
				So(report.status, ShouldEqual, "Fake Error")
				So(report.code, ShouldEqual, 99)
				So(report.retries, ShouldEqual, numRetries)

				component.AssertExpectations(t)
			})
		})

		Convey("When 10000 messages are produced", func() {
			var numErr int

			component.On("OnMessage", mock.AnythingOfType("*rbforwarder.message")).
				Return(nil).
				Times(numMessages)

			for i := 0; i < numMessages; i++ {
				if err := rbforwarder.Produce(
					[]byte("Hello World"),
					map[string]interface{}{"message_id": i},
				); err != nil {
					numErr++
				}
			}

			rbforwarder.Close()

			Convey("10000 messages should be get by the worker", func() {
				i := 0
				for range component.channel {
					if i++; i >= numMessages {
						break
					} else if i > numMessages {
						t.FailNow()
					}
				}

				So(numErr, ShouldBeZeroValue)
				So(i, ShouldEqual, numMessages)

				component.AssertExpectations(t)
			})

			Convey("10000 reports should be received", func() {
				reports := 0
				for range rbforwarder.messageHandler.GetReports() {
					reports++
				}

				So(numErr, ShouldBeZeroValue)
				So(reports, ShouldEqual, numMessages)

				component.AssertExpectations(t)
			})

			Convey("10000 reports should be received in order", func() {
				ordered := true
				messages := 0

				for report := range rbforwarder.GetOrderedReports() {
					if report.opts["message_id"] != messages {
						ordered = false
					}
					messages++
				}

				So(numErr, ShouldBeZeroValue)
				So(ordered, ShouldBeTrue)
				So(messages, ShouldEqual, numMessages)

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
			component1.On("Init", i).Return(nil)
			component2.On("Init", i).Return(nil)
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

			component1.On("OnMessage", mock.MatchedBy(func(m *message) bool {
				opt, err := m.GetOpt("message_id")
				if err != nil {
					return false
				}

				return opt.(string) == "test123"
			}))
			component2.On("OnMessage", mock.MatchedBy(func(m *message) bool {
				opt, err := m.GetOpt("message_id")
				if err != nil {
					return false
				}

				return opt.(string) == "test123"
			}))

			err1 := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
			)

			rbforwarder.Close()

			err2 := rbforwarder.Produce(
				[]byte("Hello World"),
				map[string]interface{}{"message_id": "test123"},
			)

			Convey("\"Hello World\" message should be processed by the pipeline", func() {
				reports := 0
				for report := range rbforwarder.messageHandler.GetReports() {
					reports++

					So(report.opts["message_id"], ShouldEqual, "test123")
					So(report.code, ShouldEqual, 0)
					So(report.status, ShouldEqual, "OK")
				}

				m := <-component2.channel

				So(err1, ShouldBeNil)
				So(err2, ShouldNotBeNil)
				So(reports, ShouldEqual, 1)
				So(m, ShouldEqual, "-> [Hello World] <-")

				component1.AssertExpectations(t)
				component2.AssertExpectations(t)
			})
		})
	})
}
