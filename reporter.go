package rbforwarder

import (
	"sync"
	"time"
)

// reporter is used to handle the reports produced by the last element
// of the pipeline. The first element of the pipeline can know the status
// of the produced message using GetReports() or GetOrderedReports()
type reporter struct {
	input   chan *message // Receive messages from pipeline
	retries chan *message // Send messages back to the pipeline
	out     chan *message // Send reports to the user

	queued        map[uint64]Report // Store pending reports
	currentReport uint64            // Last delivered report

	maxRetries int
	backoff    int

	wg sync.WaitGroup
}

// newReportHandler creates a new instance of reportHandler
func newReporter(
	maxRetries, backoff int,
	input, retries chan *message,
) *reporter {

	r := &reporter{
		input:   input,
		retries: retries,
		out:     make(chan *message, 100), // NOTE Temp channel size

		queued: make(map[uint64]Report),

		maxRetries: maxRetries,
		backoff:    backoff,
	}

	go func() {
		// Get reports from the handler channel
		for m := range r.input {
			// If the message has status code 0 (success) send the report to the user
			if m.code == 0 || r.maxRetries == 0 {
				r.out <- m
				continue
			}

			// If the message has status code != 0 (fail) but has been retried the
			// maximum number or retries also send it to the user
			if r.maxRetries > 0 && m.retries >= r.maxRetries {
				r.out <- m
				continue
			}

			// In othe case retry the message sending it again to the pipeline
			r.wg.Add(1)
			go func(m *message) {
				defer r.wg.Done()
				m.retries++
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

func (r *reporter) GetReports() chan Report {
	reports := make(chan Report)

	go func() {
		for message := range r.out {
			reports <- message.GetReport()
		}

		close(reports)
	}()

	return reports
}

func (r *reporter) GetOrderedReports() chan Report {
	reports := make(chan Report)

	go func() {
		for message := range r.out {
			report := message.GetReport()

			if message.seq == r.currentReport {
				// The message is the expected. Send it.
				reports <- report
				r.currentReport++
			} else {
				// This message is not the expected. Store it.
				r.queued[message.seq] = report
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

		close(reports)
	}()

	return reports
}
