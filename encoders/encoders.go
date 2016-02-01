package encoders

import (
	"github.com/Bigomby/go-pipes/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Entry

type Encoder interface {
	Init(int) error
	Encode(*util.Message) error
}

func NewEncoder(config util.ElementConfig) Encoder {
	log = util.NewLogger("encoder")

	switch config.Type {
	default:
		return &NullEncoder{
			configRaw: config,
		}
		break
	}

	return nil
}
