package sources

import (
	"github.com/Sirupsen/logrus"
	"github.com/redBorder/rbforwarder"
)

var log *logrus.Entry

// NewSource creates a new source depending on the configuration passed
// as argument
func NewSource(config rbforwarder.SourceConfig) (source rbforwarder.Source) {
	switch config.Type {
	default:
	}

	return
}
