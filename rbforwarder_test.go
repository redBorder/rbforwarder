package rbforwarder

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type TestSender struct {
	channel chan string
}
type TestSenderHelper struct {
	channel chan string
}

func (tsender *TestSender) Init(id int) error {
	return nil
}

func (tsender *TestSender) Send(m *Message) error {
	time.Sleep((time.Millisecond * 10) * time.Duration(rand.Int31n(50)))

	select {
	case tsender.channel <- string(m.OutputBuffer.Bytes()):
		m.Report(0, "OK")
	}
	return nil
}

func (tsh *TestSenderHelper) CreateSender() Sender {
	return &TestSender{
		channel: tsh.channel,
	}
}

func TestBackend(t *testing.T) {
	Convey("Given a working backend", t, func() {
		senderChannel := make(chan string, 100)
		reportChannel := make(chan Report, 100)

		senderHelper := new(TestSenderHelper)
		senderHelper.channel = senderChannel

		rbforwarder := NewRBForwarder(Config{
			Retries:   0,
			Workers:   10,
			QueueSize: 10000,
		})

		go func() {
			for report := range rbforwarder.GetOrderedReports() {
				select {
				case reportChannel <- report:
				default:
					Printf("Can't send report to report channel")
				}
			}
		}()

		rbforwarder.SetSenderHelper(senderHelper)
		rbforwarder.Start()

		Convey("When a \"Hello World\" message is received", func() {

			message, err := rbforwarder.TakeMessage()
			if err != nil {
				Printf(err.Error())
			}
			message.InputBuffer.WriteString("Hola mundo")
			if err := message.Produce(); err != nil {
				Printf(err.Error())
			}

			Convey("A \"Hello World\" message should be sent", func() {
				message := <-senderChannel
				So(message, ShouldEqual, "Hola mundo")
			})
		})
	})
}

func TestBackend2(t *testing.T) {
	Convey("Given a working backend", t, func() {
		reportChannel := make(chan Report, 100)
		senderChannel := make(chan string, 100)

		senderHelper := new(TestSenderHelper)
		senderHelper.channel = senderChannel

		rbforwarder := NewRBForwarder(Config{
			Retries:   0,
			Workers:   10,
			QueueSize: 10000,
		})

		rbforwarder.SetSenderHelper(senderHelper)
		rbforwarder.Start()

		Convey("When a \"Hello World\" message is received", func() {
			go func() {
				for report := range rbforwarder.GetOrderedReports() {
					select {
					case reportChannel <- report:
					default:
						Printf("Can't send report to report channel")
					}
				}
			}()

			message, err := rbforwarder.TakeMessage()
			if err != nil {
				Printf(err.Error())
			}
			message.InputBuffer.WriteString("Hola mundo")
			if err := message.Produce(); err != nil {
				Printf(err.Error())
			}

			Convey("A report of the sent message should be received", func() {
				report := <-reportChannel
				So(report.StatusCode, ShouldEqual, 0)
				So(report.Status, ShouldEqual, "OK")
				So(report.ID, ShouldEqual, 0)
			})
		})
	})
}

func TestBackend3(t *testing.T) {
	Convey("Given a working backend", t, func() {
		senderChannel := make(chan string, 100)
		reportChannel := make(chan Report, 100)

		senderHelper := new(TestSenderHelper)
		senderHelper.channel = senderChannel

		rbforwarder := NewRBForwarder(Config{
			Retries:   0,
			Workers:   10,
			QueueSize: 10000,
		})

		rbforwarder.SetSenderHelper(senderHelper)
		rbforwarder.Start()

		Convey("When multiple messages are received", func() {

			fmt.Println("")
			go func() {
				for report := range rbforwarder.GetOrderedReports() {
					select {
					case reportChannel <- report:
					default:
						Printf("Can't send report to report channel")
					}
				}
			}()

			for i := 0; i < 100; i++ {
				message, err := rbforwarder.TakeMessage()
				if err != nil {
					Printf(err.Error())
				}
				message.InputBuffer.WriteString("Message")
				if err := message.Produce(); err != nil {
					Printf(err.Error())
				}

				go func() {
					<-senderChannel
				}()
			}

			Convey("Reports should be delivered ordered", func() {
				var currentID uint64

			forLoop:
				for currentID = 0; currentID < 100; currentID++ {
					report := <-reportChannel
					if report.ID != currentID {
						Printf("Missmatched report. Expected %d, Got %d", currentID, report.ID)
						break forLoop
					}
				}

				So(currentID, ShouldEqual, 100)
			})
		})
	})
}
