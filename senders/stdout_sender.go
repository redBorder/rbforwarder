package senders

import (
	"errors"
	"time"

	"github.com/redBorder/rb-forwarder/util"
)

type StdoutSender struct {
	counter   int64
	id        int
	timer     *time.Timer
	rawConfig util.Config
}

func (s *StdoutSender) Init(id int) error {
	s.id = id

	go func() {
		for {
			s.timer = time.NewTimer(2 * time.Second)
			<-s.timer.C
			log.Printf("[%d] Sender: Messages per second %d", s.id, s.counter/2)
			s.counter = 0
		}
	}()

	return nil
}

func (s *StdoutSender) Send(message *util.Message) error {

	if len(string(message.OutputBuffer.Bytes())) < 1 {
		return errors.New("Invalid message")
	}

	log.Debugf("MESSAGE: %s", string(message.OutputBuffer.Bytes()))
	s.counter++

	return nil
}
