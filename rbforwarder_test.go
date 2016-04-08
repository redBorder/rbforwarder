package rbforwarder

import (
	"fmt"
	"log"
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
			message, err := f.TakeMessage()
			if err != nil {
				log.Fatal(err)
			}
			message.InputBuffer.WriteString(msg)
			if err := message.Produce(); err != nil {
				log.Fatal(err)
			}
		}
	}()

	go func() {
		for report := range f.GetOrderedReports() {
			select {
			case reportChannel <- report:
			default:
				log.Fatal("Can't send report to report channel")
			}
		}
	}()
}

func (tsource *TestSource) Close() {
	select {
	case closeChannel <- "finish":
	default:
		log.Fatal("Cant send FINISH to close channel")
	}
}

func (tsender *TestSender) Init(id int) error {
	return nil
}

func (tsender *TestSender) Send(m *Message) error {
	// TODO Mock time
	time.Sleep((time.Millisecond * 10) * time.Duration(rand.Int31n(50)))
	select {
	case senderChannel <- string(m.OutputBuffer.Bytes()):
		if err := m.Report(0, "OK"); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("Can't send message to sender channel")
	}
	return nil
}

func (tsh *TestSenderHelper) CreateSender() Sender {
	return &TestSender{}
}

func TestBackend(t *testing.T) {
	Convey("Given a working backend", t, func() {
		sourceChannel = make(chan string, 100)
		senderChannel = make(chan string, 100)
		reportChannel = make(chan Report, 100)
		closeChannel = make(chan string, 1)

		source := new(TestSource)
		senderHelper := new(TestSenderHelper)

		rbforwarder := NewRBForwarder(Config{
			Retries:   0,
			Workers:   10,
			QueueSize: 10000,
		})
		rbforwarder.SetSource(source)
		rbforwarder.SetSenderHelper(senderHelper)

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

			fmt.Println("")

			for i := 0; i < 100; i++ {
				select {
				case sourceChannel <- "Message":
				default:
					log.Fatal("Can't send message to source channel")
				}

				go func() {
					<-senderChannel
				}()
			}

			Convey("Reports should be delivered ordered", func() {
				var currentID uint64

				for currentID = 0; currentID < 100; currentID++ {
					report := <-reportChannel
					if report.ID != currentID {
						log.Fatal("Missmatched report")
					}
				}

				So(currentID, ShouldEqual, 100)
			})
		})

		rbforwarder.Close()
	})
}
