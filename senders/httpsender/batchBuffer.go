package httpsender

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/redBorder/rbforwarder/pipeline"
)

type batchBuffer struct {
	buff         *bytes.Buffer
	writer       io.Writer
	timer        *time.Timer
	mutex        *sync.Mutex
	messageCount int64
	messages     []pipeline.Messenger
}
