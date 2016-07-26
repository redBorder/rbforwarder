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
	reports chan *pipeline.Message

	status     string
	statusCode int
}

func (s *MockSender) Init(id int, reports chan *pipeline.Message) error {
	args := s.Called(id, reports)
	s.reports = reports
	return args.Error(0)
}

func (s *MockSender) OnMessage(m *pipeline.Message) error {
	s.channel <- string(m.InputBuffer.Bytes())

	m.Report.StatusCode = s.statusCode
	m.Report.Status = s.status
	s.reports <- m

	args := s.Called(m)

	return args.Error(0)
}

func TestBackend(t *testing.T) {
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
			sender.On("Init", i, rbforwarder.backend.reports).Return(nil)
		}

		rbforwarder.SetSender(sender)
		rbforwarder.Start()

		Convey("When 10000 messages are produced", func() {
			sender.On("OnMessage", mock.AnythingOfType("*pipeline.Message")).
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
					}
				}

				So(i, ShouldEqual, numMessages)

				i = 0
				for range rbforwarder.GetReports() {
					if i++; i >= numMessages {
						break
					}
				}

				sender.AssertExpectations(t)
			})

			Convey("10000 reports should be received", func() {
				i := 0
				for range rbforwarder.GetReports() {
					if i++; i >= numMessages {
						break
					}
				}

				So(i, ShouldEqual, numMessages)

				sender.AssertExpectations(t)
			})

			Convey("10000 reports should be received in order", func() {
				i := 0
				var err error

				for report := range rbforwarder.GetOrderedReports() {
					if report.Metadata["message_id"] != i {
						err = errors.New("Unexpected report: " +
							strconv.Itoa(report.Metadata["message_id"].(int)))
					}
					if i++; i >= numMessages {
						break
					}
				}

				So(err, ShouldBeNil)
				So(i, ShouldEqual, numMessages)

				sender.AssertExpectations(t)
			})
		})

		Convey("When a \"Hello World\" message is produced", func() {
			sender.status = "OK"
			sender.statusCode = 0

			if err := rbforwarder.Produce([]byte("Hello World"), map[string]interface{}{
				"message_id": "test123",
			}); err != nil {
				Printf(err.Error())
			}

			Convey("\"Hello World\" message should be get by the worker", func() {
				sender.On("OnMessage", mock.MatchedBy(func(m *pipeline.Message) bool {
					return m.Report.Metadata["message_id"] == "test123"
				})).Return(nil)

				report := <-rbforwarder.GetReports()

				So(report.Metadata["message_id"], ShouldEqual, "test123")
				So(report.StatusCode, ShouldEqual, 0)
				So(report.Status, ShouldEqual, "OK")

				sender.AssertExpectations(t)
			})
		})

		Convey("When a message fails to send", func() {
			sender.status = "Fake Error"
			sender.statusCode = 99

			if err := rbforwarder.Produce([]byte("Hello World"), map[string]interface{}{
				"message_id": "test123",
			}); err != nil {
				Printf(err.Error())
			}

			Convey("The message should be retried", func() {
				sender.On("OnMessage", mock.MatchedBy(func(m *pipeline.Message) bool {
					return m.Report.Metadata["message_id"] == "test123"
				})).Return(nil)

				report := <-rbforwarder.GetOrderedReports()

				So(report.Metadata["message_id"], ShouldEqual, "test123")
				So(report.Status, ShouldEqual, "Fake Error")
				So(report.StatusCode, ShouldEqual, 99)
				So(report.Retries, ShouldEqual, numRetries)

				sender.AssertExpectations(t)
			})
		})

		Reset(func() {
			rbforwarder.Close()
		})
	})
}
