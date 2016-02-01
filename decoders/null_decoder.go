package decoders

import (
	"github.com/Bigomby/go-pipes/util"
)

type NullDecoder struct {
	id        int
	configRaw util.ElementConfig
}

// Creates a Decoder that does nothing
func (d NullDecoder) Init(id int) error {
	d.id = id
	log.Debugf("[%d] Started Null Decoder", d.id)
	return nil
}

// Does nothing
func (d NullDecoder) Decode(message *util.Message) error {
	message.Data = message.InputBuffer
	return nil
}
