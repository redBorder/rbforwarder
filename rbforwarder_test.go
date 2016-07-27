package rbforwarder

import (
	"errors"
	"strconv"
	"testing"

	"github.com/redBorder/rbforwarder/pipeline"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
)

type MockSender struct {
	mock.Mock

	channel chan string

	status     string
	statusCode int
}

func (s *MockSender) Init(id int) error {
	args := s.Called(id)
	return args.Error(0)
}

func (s *MockSender) OnMessage(m pipeline.Messenger) {
	data, _ := m.PopData()
	s.channel <- string(data)
	m.Done(s.statusCode, s.status)
	s.Called(m)
}

func TestRBForwarder(t *testing.T) {
	Convey("Given a working pipeline", t, func() {
		numMessages := 10000
		numWorkers := 10
		numRetries := 3

		sender := &MockSender{
			channel: make(chan string, 10000),
		}

		rbforwarder := NewRBForwarder(Config{
			Retries:   numRetries,
			Workers:   numWorkers,
			QueueSize: numMessages,
		})

		for i := 0; i < numWorkers; i++ {
			sender.On("Init", i).Return(nil)
		}

		rbforwarder.SetSender(sender)

		Convey("When a \"Hello World\" message is produced", func() {
			sender.status = "OK"
			sender.statusCode = 0

			sender.On("OnMessage", mock.MatchedBy(func(m *message) bool {
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

				sender.AssertExpectations(t)
			})
		})

		Convey("When calling OnMessage() with options", func() {
			sender.On("OnMessage", mock.MatchedBy(func(m *message) bool {
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

				sender.AssertExpectations(t)
			})
		})

		Convey("When calling OnMessage() without options", func() {
			sender.On("OnMessage", mock.MatchedBy(func(m *message) bool {
				_, err := m.GetOpt("nonexistent")
				return err != nil
			}))

			Convey("Should not be possible to read the option", func() {
				if err := rbforwarder.Produce([]byte("Hello World"), nil); err != nil {
					Printf(err.Error())
				}

				report := <-rbforwarder.GetReports()

				So(report.opts, ShouldBeNil)

				sender.AssertExpectations(t)
			})
		})

		Convey("When a message fails to send", func() {
			sender.status = "Fake Error"
			sender.statusCode = 99

			sender.On("OnMessage", mock.MatchedBy(func(m *message) bool {
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

				sender.AssertExpectations(t)
			})
		})

		Convey("When 10000 messages are produced", func() {
			sender.On("OnMessage", mock.AnythingOfType("*rbforwarder.message")).
				Return(nil).
				Times(numMessages)

			for i := 0; i < numMessages; i++ {
				if err := rbforwarder.Produce([]byte(""), map[string]interface{}{
					"message_id": i,
				}); err != nil {
					Printf(err.Error())
				}
			}

			Convey("10000 messages should be get by the worker", func() {
				i := 0
				for range sender.channel {
					if i++; i >= numMessages {
						break
					} else if i > numMessages {
						t.FailNow()
					}
				}

				So(i, ShouldEqual, numMessages)

				i = 0
				for range rbforwarder.GetReports() {
					if i++; i >= numMessages {
						break
					} else if i > numMessages {
						t.FailNow()
					}
				}

				sender.AssertExpectations(t)
			})

			Convey("10000 reports should be received", func() {
				i := 0
				for range rbforwarder.GetReports() {
					if i++; i == numMessages {
						break
					} else if i > numMessages {
						t.FailNow()
					}
				}

				So(i, ShouldEqual, numMessages)

				sender.AssertExpectations(t)
			})

			Convey("10000 reports should be received in order", func() {
				i := 0
				var err error

				for report := range rbforwarder.GetOrderedReports() {
					if report.opts["message_id"] != i {
						err = errors.New("Unexpected report: " +
							strconv.Itoa(report.opts["message_id"].(int)))
					}
					if i++; i >= numMessages {
						break
					} else if i > numMessages {
						t.FailNow()
					}
				}

				So(err, ShouldBeNil)
				So(i, ShouldEqual, numMessages)

				sender.AssertExpectations(t)
			})
		})

		Reset(func() {
			rbforwarder.Close()
		})
	})
}
