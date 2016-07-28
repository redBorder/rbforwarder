package httpsender

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"crypto/tls"
	"net/http"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/redBorder/rbforwarder/pipeline"
)

// Sender receives data from pipe and send it via HTTP to an endpoint
type Sender struct {
	id          int
	client      *http.Client
	batchBuffer map[string]*batchBuffer

	// Statistics
	counter int64
	timer   *time.Timer

	// Configuration
	logger *logrus.Entry
	config Config
}

// Init initializes an HTTP sender
func (s *Sender) Init(id int) error {
	s.id = id

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
func (s *Sender) OnMessage(message pipeline.Messenger) error {

	// logger.Printf("[%d] Sending message ID: [%d]", s.id, message)

	// We can send batch only for messages with the same path
	var path string

	if opt, err := message.GetOpt(s.config.Endpoint); err == nil {
		path = opt.(string)
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
	data, err := message.PopData()
	if err != nil {
		return err
	}

	if _, err := batchBuffer.writer.Write(data); err != nil {
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
		// TODO Mark message as done
		// for _, message := range batchBuffer.messages {
		// message.Done(errRequest, err.Error())
		// }
		return
	}

	// Use proper header for sending deflate
	if s.config.Deflate {
		req.Header.Add("Content-Encoding", "deflate")
	}

	// Send the HTTP POST request
	res, err := s.client.Do(req)
	if err != nil {
		// TODO Mark message as done
		// for _, message := range batchBuffer.messages {
		// message.Done(errHTTP, err.Error())
		// }
		return
	}
	defer res.Body.Close()

	// Send the reports
	if res.StatusCode >= 400 {
		// TODO Mark message as done
		// for _, message := range batchBuffer.messages {
		// message.Done(errStatus, res.Status)
		// }
	} else {
		// TODO Mark message as done
		// for _, message := range batchBuffer.messages {
		// message.Done(0, res.Status)
		// }
	}

	// Statistics
	s.counter += batchBuffer.messageCount
}
