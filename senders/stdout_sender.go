package senders

import (
	"errors"
	"time"

	"github.com/redBorder/rb-forwarder/util"
)

type StdoutSender struct {
	counter   int64
	timer     *time.Timer
	rawConfig util.Config
}

func (s *StdoutSender) Init(i int) error {
	go func() {
		for {
			s.timer = time.NewTimer(2 * time.Second)
			<-s.timer.C
			log.Printf("Sender: Messages per second %d\n", s.counter/2)
			s.counter = 0
		}
	}()
	return nil
}

func (s *StdoutSender) Send(message *util.Message) error {

	if len(string(message.OutputBuffer.Bytes())) < 1 {
		return errors.New("Invalid message")
	}

	s.counter++

	return nil
}
