package rbforwarder

import (
	"errors"
	"strconv"
	"testing"

	"github.com/redBorder/rbforwarder/pipeline"
	. "github.com/smartystreets/goconvey/convey"
)

type TestSender struct {
	channel chan string
	reports chan *pipeline.Message
}

func (tsender *TestSender) Init(id int, reports chan *pipeline.Message) error {
	tsender.reports = reports
	return nil
}

func (tsender *TestSender) OnMessage(m *pipeline.Message) error {
	// time.Sleep((time.Millisecond * 10) * time.Duration(rand.Int31n(50)))

	tsender.channel <- string(m.InputBuffer.Bytes())
	m.Report.StatusCode = 0
	m.Report.Status = "OK"
	tsender.reports <- m

	return nil
}

func TestBackend(t *testing.T) {
	Convey("Given a working pipeline", t, func() {
		numMessages := 10000

		sender := &TestSender{
			channel: make(chan string, 10000),
		}

		rbforwarder := NewRBForwarder(Config{
			Retries:   1,
			Workers:   10,
			QueueSize: 10000,
		})

		rbforwarder.SetSender(sender)
		rbforwarder.Start()

		Convey("When 10000 messages are produced", func() {
			go func() {
				for i := 0; i < numMessages; i++ {
					if err := rbforwarder.Produce([]byte(""), map[string]interface{}{
						"message_id": i,
					}); err != nil {
						Printf(err.Error())
					}
				}
			}()

			Convey("10000 messages should be get by the worker", func() {
				i := 0
				for range sender.channel {
					if i++; i >= numMessages {
						break
					}
				}

				So(i, ShouldEqual, numMessages)
			})

			Convey("10000 reports should be received", func() {
				i := 0
				for range rbforwarder.GetReports() {
					if i++; i >= numMessages {
						break
					}
				}

				So(i, ShouldEqual, numMessages)
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
			})
		})

		Convey("When a \"Hello World\" message is produced", func() {
			if err := rbforwarder.Produce([]byte("Hello World"), map[string]interface{}{
				"message_id": "test123",
			}); err != nil {
				Printf(err.Error())
			}

			Convey("\"Hello World\" message should be get by the worker", func() {
				message := <-sender.channel
				So(message, ShouldEqual, "Hello World")
			})

			Convey("A report of the \"Hello World\" message should be received", func() {
				report := <-rbforwarder.GetReports()
				So(report.StatusCode, ShouldEqual, 0)
				So(report.Status, ShouldEqual, "OK")
				So(report.ID, ShouldEqual, 0)
				So(report.Metadata["message_id"], ShouldEqual, "test123")
			})
		})

		Reset(func() {
			rbforwarder.Close()
		})
	})
}
