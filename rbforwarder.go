package rbforwarder

import (
	"errors"
	"sync/atomic"

	"github.com/Sirupsen/logrus"
	"github.com/oleiade/lane"
	"github.com/redBorder/rbforwarder/utils"
)

// Version is the current tag
var Version = "0.4-beta3"

var log = logrus.New()

// Logger for the package
var Logger = logrus.NewEntry(log)

// RBForwarder is the main objecto of the package. It has the main methods for
// send messages and get reports. It has a backend for routing messages between
// workers
type RBForwarder struct {
	p *pipeline
	r *reportHandler

	currentProducedID uint64
	working           uint32
}

// NewRBForwarder creates a new Forwarder object
func NewRBForwarder(config Config) *RBForwarder {
	produces := make(chan *utils.Message, config.QueueSize)
	retries := make(chan *utils.Message, config.QueueSize)
	reports := make(chan *utils.Message, config.QueueSize)

	f := &RBForwarder{
		working: 1,
		p:       newPipeline(produces, retries, reports),
		r: newReporter(
			config.Retries,
			config.Backoff,
			reports,
			retries,
		),
	}

	fields := logrus.Fields{
		"retries":      config.Retries,
		"backoff_time": config.Backoff,
		"queue_size":   config.QueueSize,
	}

	Logger.WithFields(fields).Debug("Initialized rB Forwarder")

	return f
}

// Close stops pending actions
func (f *RBForwarder) Close() {
	atomic.StoreUint32(&f.working, 0)
	close(f.p.input)
}

// PushComponents adds a new component to the pipeline
func (f *RBForwarder) PushComponents(components []utils.Composer, w []int) {
	for i, component := range components {
		f.p.PushComponent(component, w[i])
	}
}

// GetReports is used by the source to get a report for a sent message.
// Reports are delivered on the same order that was sent
func (f *RBForwarder) GetReports() <-chan interface{} {
	return f.r.GetReports()
}

// GetOrderedReports is the same as GetReports() but the reports are delivered
// in order
func (f *RBForwarder) GetOrderedReports() <-chan interface{} {
	return f.r.GetOrderedReports()
}

// Produce is used by the source to send messages to the backend
func (f *RBForwarder) Produce(data []byte, opts map[string]interface{}, opaque interface{}) error {
	if atomic.LoadUint32(&f.working) == 0 {
		return errors.New("Forwarder has been closed")
	}

	seq := f.currentProducedID
	f.currentProducedID++
	m := utils.NewMessage()
	r := report{
		seq:    seq,
		opaque: lane.NewStack(),
	}

	m.PushPayload(data)
	m.Opts = opts
	m.Reports.Push(r)
	r.opaque.Push(opaque)

	f.p.input <- m

	return nil
}
