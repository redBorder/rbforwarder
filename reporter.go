package rbforwarder

import "time"

// reporter is used to handle the reports produced by the last element
// of the pipeline. The first element of the pipeline can know the status
// of the produced message using GetReports() or GetOrderedReports()
type reporter struct {
	input    chan *message // Receive messages from pipeline
	pipeline chan *message // Send messages back to the pipeline
	out      chan *message // Send reports to the user

	queued        map[uint64]Report // Store pending reports
	currentReport uint64            // Last delivered report

	maxRetries int
	backoff    int
}

// newReportHandler creates a new instance of reportHandler
func newReporter(
	maxRetries, backoff int,
	input, pipeline chan *message,
) *reporter {

	r := &reporter{
		input:    input,
		pipeline: pipeline,
		out:      make(chan *message, 100),

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
			go func(m *message) {
				m.retries++
				Logger.
					WithField("Seq", m.seq).
					WithField("Retry", m.retries).
					WithField("Status", m.status).
					WithField("Code", m.code).
					Warnf("Retrying message")

				<-time.After(time.Duration(r.backoff) * time.Second)
				r.pipeline <- m
			}(m)
		}

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
