package senders

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"crypto/tls"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/redBorder/rb-forwarder/util"
)

// HTTPSender receives data from pipe and send it via HTTP to an endpoint
type HTTPSender struct {
	id          int
	client      *http.Client
	batchBuffer map[string]*BatchBuffer

	// Statistics
	counter int64
	timer   *time.Timer

	// Configuration
	rawConfig util.Config
	config    httpSenderConfig
}

// BatchBuffer stores multiple messages to send it in one HTTP Post body
type BatchBuffer struct {
	buff         *bytes.Buffer
	writer       io.Writer
	timer        *time.Timer
	mutex        *sync.Mutex
	messageCount int64
}

type httpSenderConfig struct {
	URL          string
	IgnoreCert   bool
	Deflate      bool
	ShowCounter  int
	BatchSize    int64
	BatchTimeout time.Duration
}

// Init initializes an HTTP sender
func (s *HTTPSender) Init(id int) error {
	s.parseConfig()
	s.id = id

	// Create the client object. Useful for skipping SSL verify
	tr := &http.Transport{}
	if s.config.IgnoreCert {
		log.Warn("Ignoring SSL certificates")
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	s.client = &http.Client{Transport: tr}

	log.Debugf("[%d] HTTP sender ready", s.id)

	// A map to store buffers for each endpoint
	s.batchBuffer = make(map[string]*BatchBuffer)

	if s.config.ShowCounter > 0 {
		go func() {
			for {
				timer := time.NewTimer(time.Duration(s.config.ShowCounter) * time.Second)
				<-timer.C
				log.Infof("[%d] Sender: Messages per second %d", s.id, s.counter/int64(s.config.ShowCounter))
				if s.counter == 0 {
					log.Warnf("[%d] No messages sent", s.id)
				}
				s.counter = 0
			}
		}()
	}

	return nil
}

// Send stores a message received from the pipeline into a buffer to perform
// batching.
func (s *HTTPSender) Send(message *util.Message) error {

	// We can send batch only for messages with the same path
	path := message.Attributes["path"]

	// Initialize buffer for path
	if _, exists := s.batchBuffer[path]; !exists {
		s.batchBuffer[path] = &BatchBuffer{
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
		return err
	}
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

func (s *HTTPSender) batchSend(batchBuffer *BatchBuffer, path string) {

	// Stop the timeout timer
	batchBuffer.timer.Stop()

	// Make sure the writer is closed
	if s.config.Deflate {
		batchBuffer.writer.(*zlib.Writer).Close()
	}

	// Create the HTTP POST request
	req, err := http.NewRequest("POST", s.config.URL+"/"+path, batchBuffer.buff)
	if err != nil {
		log.Errorf("Error creating request: %s", err.Error())
		goto FINISH
	}

	// Use proper header for sending deflate
	if s.config.Deflate {
		req.Header.Add("Content-Encoding", "deflate")
	}

	// Send the HTTP POST request
	_, err = s.client.Do(req)
	if err != nil {
		log.Errorf("Error sending request: %s", err.Error())
		goto FINISH
	}

	log.Debugf("Sending %d messages to %s", batchBuffer.messageCount, s.config.URL+"/"+path)

	// Statistics
	s.counter += batchBuffer.messageCount

FINISH:

	// Reset buffer and clear message counter
	batchBuffer.messageCount = 0
	batchBuffer.buff = new(bytes.Buffer)

	// Reset writers
	if s.config.Deflate {
		batchBuffer.writer = zlib.NewWriter(batchBuffer.buff)
	} else {
		batchBuffer.writer = bufio.NewWriter(batchBuffer.buff)
	}

	// Reset timeout timer
	s.batchBuffer[path].timer.Reset(s.config.BatchTimeout)
}

// Parse the config from YAML file
func (s *HTTPSender) parseConfig() {
	if s.rawConfig["url"] != nil {
		s.config.URL = s.rawConfig["url"].(string)
	} else {
		log.Fatal("No url provided")
	}

	if s.rawConfig["insecure"] != nil {
		s.config.IgnoreCert = s.rawConfig["insecure"].(bool)
	}
	if s.rawConfig["batchsize"] != nil {
		s.config.BatchSize = int64(s.rawConfig["batchsize"].(int))
	} else {
		s.config.BatchSize = 1
	}
	if s.rawConfig["batchtimeout"] != nil {
		s.config.BatchTimeout = time.Duration(s.rawConfig["batchtimeout"].(int)) * time.Millisecond
	}
	if s.rawConfig["deflate"] != nil {
		s.config.Deflate = s.rawConfig["deflate"].(bool)
	}
	if s.rawConfig["showcounter"] != nil {
		s.config.ShowCounter = s.rawConfig["showcounter"].(int)
	}
}
