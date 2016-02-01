package encoders

import (
	"bytes"
	"errors"

	"github.com/Bigomby/go-pipes/util"
)

type NullEncoder struct {
	id        int
	configRaw util.ElementConfig
}

// Creates a Encoder that does nothing
func (d NullEncoder) Init(i int) error {
	d.id = i
	log.Debugf("[%d] Started Null Encoder", d.id)
	return nil
}

// Does nothing
func (d NullEncoder) Encode(message *util.Message) error {
	switch message.Data.(type) {
	case *bytes.Buffer:
		message.OutputBuffer = message.Data.(*bytes.Buffer)
		break
	default:
		return errors.New("Null marshaller cant decode message data")
	}

	return nil
}
