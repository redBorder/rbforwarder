package httpsender

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"crypto/tls"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/redBorder/rbforwarder"
)

const (
	errRequest = 101
	errStatus  = 102
	errHTTP    = 103
)

// Sender receives data from pipe and send it via HTTP to an endpoint
type Sender struct {
	id          int
	client      *http.Client
	batchBuffer map[string]*batchBuffer
	reports     chan *rbforwarder.Message

	// Statistics
	counter int64
	timer   *time.Timer

	// Configuration
	logger *logrus.Entry
	config config
}

type batchBuffer struct {
	buff         *bytes.Buffer
	writer       io.Writer
	timer        *time.Timer
	mutex        *sync.Mutex
	messageCount int64
	messages     []*rbforwarder.Message
}

type config struct {
	URL          string
	Endpoint     string
	IgnoreCert   bool
	Deflate      bool
	ShowCounter  int
	BatchSize    int64
	BatchTimeout time.Duration
}

// Init initializes an HTTP sender
func (s *Sender) Init(id int, reports chan *rbforwarder.Message) error {
	s.id = id
	s.reports = reports

	// Create the client object. Useful for skipping SSL verify
	tr := &http.Transport{}
	if s.config.IgnoreCert {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	s.client = &http.Client{Transport: tr}

	// A map to store buffers for each endpoint
	s.batchBuffer = make(map[string]*batchBuffer)

	// Show the messages per second every "showCounter" seconds
	if s.config.ShowCounter > 0 {
		go func() {
			for {
				timer := time.NewTimer(time.Duration(s.config.ShowCounter) * time.Second)
				<-timer.C
				s.logger.WithField("worker", s.id).Infof("Messages per second %d", s.counter/int64(s.config.ShowCounter))
				s.counter = 0
			}
		}()
	}

	return nil
}

// OnMessage stores a message received from the pipeline into a buffer to perform
// batching.
func (s *Sender) OnMessage(message *rbforwarder.Message) error {

	// logger.Printf("[%d] Sending message ID: [%d]", s.id, message)

	// We can send batch only for messages with the same path
	var path string

	if message.Metadata[s.config.Endpoint] != nil {
		path = message.Metadata[s.config.Endpoint].(string)
	}

	// Initialize buffer for path
	if _, exists := s.batchBuffer[path]; !exists {
		s.batchBuffer[path] = &batchBuffer{
			mutex:        &sync.Mutex{},
			messageCount: 0,
			buff:         new(bytes.Buffer),
			timer:        time.NewTimer(s.config.BatchTimeout),
		}

		if s.config.Deflate {
			s.batchBuffer[path].writer = zlib.NewWriter(s.batchBuffer[path].buff)
		} else {
			s.batchBuffer[path].writer = bufio.NewWriter(s.batchBuffer[path].buff)
		}

		// A go rutine for send all the messages stored on the buffer when a timeout
		// occurred
		if s.config.BatchTimeout != 0 {
			go func() {
				for {
					<-s.batchBuffer[path].timer.C
					s.batchBuffer[path].mutex.Lock()
					if s.batchBuffer[path].messageCount > 0 {
						s.batchSend(s.batchBuffer[path], path)
					}
					s.batchBuffer[path].mutex.Unlock()
					s.batchBuffer[path].timer.Reset(s.config.BatchTimeout)
				}
			}()
		}
	}

	// Once the buffer is created, it's necessary to lock so a new message can't be
	// writed to buffer meanwhile the timeout go rutine is sending a request
	batchBuffer := s.batchBuffer[path]
	batchBuffer.mutex.Lock()

	// Write the new message to the buffer and increase the number of messages in
	// the buffer
	if _, err := batchBuffer.writer.Write(message.OutputBuffer.Bytes()); err != nil {
		s.logger.Error(err)
	}
	batchBuffer.messages = append(batchBuffer.messages, message)
	batchBuffer.messageCount++

	// Flush writers
	if s.config.Deflate {
		batchBuffer.writer.(*zlib.Writer).Flush()
	} else {
		batchBuffer.writer.(*bufio.Writer).Flush()
	}

	// If there are enough messages on buffer it's time to send the POST
	if batchBuffer.messageCount >= s.config.BatchSize {
		s.batchSend(batchBuffer, path)
	}
	batchBuffer.mutex.Unlock()

	return nil
}

func (s *Sender) batchSend(batchBuffer *batchBuffer, path string) {

	// Reset buffer and clear message counter
	defer func() {
		batchBuffer.messageCount = 0
		batchBuffer.buff = new(bytes.Buffer)

		// Reset writers
		if s.config.Deflate {
			batchBuffer.writer = zlib.NewWriter(batchBuffer.buff)
		} else {
			batchBuffer.writer = bufio.NewWriter(batchBuffer.buff)
		}

		// Reset timeout timer
		batchBuffer.timer.Reset(s.config.BatchTimeout)
		batchBuffer.messages = nil
	}()

	// Stop the timeout timer
	batchBuffer.timer.Stop()

	// Make sure the writer is closed
	if s.config.Deflate {
		batchBuffer.writer.(*zlib.Writer).Close()
	}

	// Create the HTTP POST request
	req, err := http.NewRequest("POST", s.config.URL+"/"+path, batchBuffer.buff)
	if err != nil {
		s.logger.Errorf("Error creating request: %s", err.Error())
		for _, message := range batchBuffer.messages {
			message.Report.StatusCode = errRequest
			message.Report.Status = err.Error()
			s.reports <- message
		}
		return
	}

	// Use proper header for sending deflate
	if s.config.Deflate {
		req.Header.Add("Content-Encoding", "deflate")
	}

	// Send the HTTP POST request
	res, err := s.client.Do(req)
	if err != nil {
		for _, message := range batchBuffer.messages {
			message.Report.StatusCode = errHTTP
			message.Report.Status = err.Error()
			s.reports <- message
		}
		return
	}
	defer res.Body.Close()

	// Send the reports
	if res.StatusCode >= 400 {
		for _, message := range batchBuffer.messages {
			message.Report.StatusCode = errStatus
			message.Report.Status = res.Status
			s.reports <- message
		}
	} else {
		for _, message := range batchBuffer.messages {
			message.Report.StatusCode = 0
			message.Report.Status = res.Status
			s.reports <- message
		}
	}

	// Statistics
	s.counter += batchBuffer.messageCount
}
