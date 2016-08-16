package rbforwarder

import (
	"sync"
	"time"

	"github.com/redBorder/rbforwarder/utils"
)

// reportHandler is used to handle the reports produced by the last element
// of the pipeline. The first element of the pipeline can know the status
// of the produced message using GetReports() or GetOrderedReports()
type reportHandler struct {
	input   chan *utils.Message // Receive messages from pipeline
	retries chan *utils.Message // Send messages back to the pipeline
	out     chan *utils.Message // Send reports to the user

	queued        map[uint64]interface{} // Store pending reports
	currentReport uint64                 // Last delivered report

	maxRetries int
	backoff    int

	wg sync.WaitGroup
}

// newReportHandler creates a new instance of reportHandler
func newReporter(
	maxRetries, backoff int,
	input, retries chan *utils.Message,
) *reportHandler {

	r := &reportHandler{
		input:   input,
		retries: retries,
		out:     make(chan *utils.Message, 100), // NOTE Temp channel size

		queued: make(map[uint64]interface{}),

		maxRetries: maxRetries,
		backoff:    backoff,
	}

	go func() {
		// Get reports from the handler channel
		for m := range r.input {
			// If the message has status code 0 (success) send the report to the user
			rep := m.Reports.Head().(Report)
			if rep.Code == 0 || r.maxRetries == 0 {
				r.out <- m
				continue
			}

			// If the message has status code != 0 (fail) but has been retried the
			// maximum number or retries also send it to the user
			if r.maxRetries > 0 && rep.retries >= r.maxRetries {
				r.out <- m
				continue
			}

			// In other case retry the message sending it again to the pipeline
			r.wg.Add(1)
			go func(m *utils.Message) {
				defer r.wg.Done()
				rep := m.Reports.Pop().(Report)
				rep.retries++
				m.Reports.Push(rep)
				<-time.After(time.Duration(r.backoff) * time.Second)
				r.retries <- m
			}(m)
		}

		r.wg.Wait()
		close(r.retries)
		close(r.out)
	}()

	Logger.Debug("Message Handler ready")

	return r
}

func (r *reportHandler) GetReports() chan interface{} {
	reports := make(chan interface{})

	go func() {
		for message := range r.out {
			for !message.Reports.Empty() {
				rep := message.Reports.Pop().(Report)
				reports <- rep
			}
		}

		close(reports)
	}()

	return reports
}

func (r *reportHandler) GetOrderedReports() chan interface{} {
	reports := make(chan interface{})

	go func() {
		for message := range r.out {
			for !message.Reports.Empty() {
				rep := message.Reports.Pop().(Report)
				if rep.seq == r.currentReport {
					// The message is the expected. Send it.
					reports <- rep
					r.currentReport++
				} else {
					// This message is not the expected. Store it.
					r.queued[rep.seq] = rep
				}

				// Check if there are stored messages and send them.
				for {
					if currentReport, ok := r.queued[r.currentReport]; ok {
						reports <- currentReport
						delete(r.queued, r.currentReport)
						r.currentReport++
					} else {
						break
					}
				}
			}
		}

		close(reports)
	}()

	return reports
}
