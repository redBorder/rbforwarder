package rbforwarder

import (
	"testing"

	"github.com/redBorder/rbforwarder/pipeline"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
)

type MockComponent struct {
	mock.Mock

	channel chan string

	status     string
	statusCode int
}

func (s *MockComponent) Init(id int) error {
	args := s.Called(id)
	return args.Error(0)
}

func (s *MockComponent) OnMessage(
	m pipeline.Messenger,
	next pipeline.Next,
	done pipeline.Done,
) {

	data, _ := m.PopData()
	s.channel <- string(data)
	done(m, s.statusCode, s.status)
	s.Called(m)
}

func TestRBForwarder(t *testing.T) {
	Convey("Given a working pipeline", t, func() {
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

		var components []pipeline.Composer
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

			if err := rbforwarder.Produce([]byte("Hello World"), map[string]interface{}{
				"message_id": "test123",
			}); err != nil {
				Printf(err.Error())
			}

			Convey("\"Hello World\" message should be get by the worker", func() {
				report := <-rbforwarder.GetReports()

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
				if err := rbforwarder.Produce([]byte("Hello World"), map[string]interface{}{}); err != nil {
					Printf(err.Error())
				}

				report := <-rbforwarder.GetReports()

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
				if err := rbforwarder.Produce([]byte("Hello World"), nil); err != nil {
					Printf(err.Error())
				}

				report := <-rbforwarder.GetReports()

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

			if err := rbforwarder.Produce([]byte("Hello World"), map[string]interface{}{
				"message_id": "test123",
			}); err != nil {
				Printf(err.Error())
			}

			Convey("The message should be retried\n", func() {
				report := <-rbforwarder.GetReports()

				So(report.opts["message_id"], ShouldEqual, "test123")
				So(report.status, ShouldEqual, "Fake Error")
				So(report.code, ShouldEqual, 99)
				So(report.retries, ShouldEqual, numRetries)

				component.AssertExpectations(t)
			})
		})

		Convey("When 10000 messages are produced", func() {
			component.On("OnMessage", mock.AnythingOfType("*rbforwarder.message")).
				Return(nil).
				Times(numMessages)

			for i := 0; i < numMessages; i++ {
				if err := rbforwarder.Produce([]byte(""), map[string]interface{}{
					"message_id": i,
				}); err != nil {
					Printf(err.Error())
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

				So(i, ShouldEqual, numMessages)

				component.AssertExpectations(t)
			})

			Convey("10000 reports should be received", func() {
				reports := 0
				for range rbforwarder.messageHandler.GetReports() {
					reports++
				}

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

				So(ordered, ShouldBeTrue)
				So(messages, ShouldEqual, numMessages)

				component.AssertExpectations(t)
			})
		})
	})
}
