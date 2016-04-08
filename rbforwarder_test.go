package rbforwarder

import (
	"math/rand"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type TestSource struct{}
type TestSender struct{}
type TestSenderHelper struct{}

var sourceChannel chan string
var senderChannel chan string
var reportChannel chan Report
var closeChannel chan string

func (tsource *TestSource) Listen(f *RBForwarder) {
	go func() {
		for msg := range sourceChannel {
			message, _ := f.TakeMessage()
			message.InputBuffer.WriteString(msg)
			if err := message.Produce(); err != nil {
				return
			}
		}
	}()

	go func() {
		for report := range f.GetOrderedReports() {
			reportChannel <- report
		}
	}()
}

func (tsource *TestSource) Close() {
	closeChannel <- "finish"
}

func (tsender *TestSender) Init(id int) error {
	return nil
}

func (tsender *TestSender) Send(m *Message) error {
	// TODO Mock time
	time.Sleep((time.Millisecond * 10) * time.Duration(rand.Int31n(50)))
	m.Report(0, "OK")
	senderChannel <- string(m.OutputBuffer.Bytes())
	return nil
}

func (tsh *TestSenderHelper) CreateSender() Sender {
	return &TestSender{}
}

func TestBackend(t *testing.T) {
	Convey("Given a working backend", t, func() {
		config := Config{
			Retries:   0,
			Workers:   10,
			QueueSize: 100,
		}
		source := new(TestSource)
		senderHelper := new(TestSenderHelper)

		rbforwarder := NewRBForwarder(config)
		rbforwarder.SetSource(source)
		rbforwarder.SetSenderHelper(senderHelper)

		sourceChannel = make(chan string, 10)
		senderChannel = make(chan string, 10)
		reportChannel = make(chan Report, 10)
		closeChannel = make(chan string, 10)

		rbforwarder.Start()

		Convey("When a \"Hello World\" message is received", func() {
			sourceChannel <- "Hola mundo"

			Convey("A \"Hello World\" message should be sent", func() {
				message := <-senderChannel
				So(message, ShouldEqual, "Hola mundo")
			})

			Convey("A report of the sent message should be received", func() {
				report := <-reportChannel
				So(report.StatusCode, ShouldEqual, 0)
				So(report.Status, ShouldEqual, "OK")
				So(report.ID, ShouldEqual, 0)
			})
		})

		Convey("When multiple messages are received", func() {
			sourceChannel <- "Message 1"
			sourceChannel <- "Message 2"
			sourceChannel <- "Message 3"
			sourceChannel <- "Message 4"
			sourceChannel <- "Message 5"
			sourceChannel <- "Message 6"

			<-senderChannel
			<-senderChannel
			<-senderChannel
			<-senderChannel
			<-senderChannel
			<-senderChannel

			Convey("Reports should be delivered ordered", func() {
				report1 := <-reportChannel
				report2 := <-reportChannel
				report3 := <-reportChannel
				report4 := <-reportChannel
				report5 := <-reportChannel
				report6 := <-reportChannel
				So(report1.ID, ShouldEqual, 0)
				So(report2.ID, ShouldEqual, 1)
				So(report3.ID, ShouldEqual, 2)
				So(report4.ID, ShouldEqual, 3)
				So(report5.ID, ShouldEqual, 4)
				So(report6.ID, ShouldEqual, 5)
			})
		})
	})
}
