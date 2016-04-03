package rbforwarder

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type TestSource struct{}
type TestSender struct{}
type TestSenderHelper struct{}

var sourceChannel = make(chan string)
var senderChannel = make(chan string)
var reportChannel = make(chan Report)
var closeChannel = make(chan string)

func (tsource *TestSource) Listen(f Forwarder) {
	go func() {
		for msg := range sourceChannel {
			message, _ := f.TakeMessage()
			message.InputBuffer.WriteString(msg)
			message.Produce()
		}
	}()

	go func() {
		for report := range f.GetReports() {
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
	senderChannel <- string(m.OutputBuffer.Bytes())
	m.Report(0, "OK")
	return nil
}

func (tsh *TestSenderHelper) CreateSender() Sender {
	return &TestSender{}
}

func TestBackend(t *testing.T) {
	Convey("Given a working backend", t, func() {
		config := Config{
			Retries:   1,
			Workers:   1,
			QueueSize: 100,
		}
		source := new(TestSource)
		senderHelper := new(TestSenderHelper)

		rbforwarder := NewRBForwarder(config)
		rbforwarder.SetSource(source)
		rbforwarder.SetSenderHelper(senderHelper)

		go rbforwarder.Start()

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
			})
		})

		Convey("When closing the rbforwarder", func() {
			rbforwarder.Close()

			Convey("The rfborwarder should exit", func() {
				So(<-closeChannel, ShouldEqual, "finish")
			})
		})
	})
}
