package rbforwarder

import "time"

// reportHandler is used to handle the reports produced by the last element
// of the pipeline. The first element of the pipeline can know the status
// of the produced message using GetReports() or GetOrderedReports()
type messageHandler struct {
	handler  chan *message // Receive messages from pipeline
	pipeline chan *message // Send messages back to the pipeline
	out      chan *message // Send reports to the user

	queued        map[uint64]Report // Store pending reports
	currentReport uint64            // Last delivered report

	maxRetries int
	backoff    int
}

// newReportHandler creates a new instance of reportHandler
func newMessageHandler(
	maxRetries, backoff int,
	handler, pipeline chan *message,
) *messageHandler {

	mh := &messageHandler{
		handler:  handler,
		pipeline: pipeline,
		out:      make(chan *message, 100),

		queued: make(map[uint64]Report),

		maxRetries: maxRetries,
		backoff:    backoff,
	}

	go func() {
		// Get reports from the handler channel
		for m := range mh.handler {
			// If the message has status code 0 (success) send the report to the user
			if m.code == 0 || mh.maxRetries == 0 {
				mh.out <- m
				continue
			}

			// If the message has status code != 0 (fail) but has been retried the
			// maximum number or retries also send it to the user
			if mh.maxRetries > 0 && m.retries >= mh.maxRetries {
				mh.out <- m
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

				<-time.After(time.Duration(mh.backoff) * time.Second)
				mh.pipeline <- m
			}(m)
		}

		close(mh.out)
	}()

	Logger.Debug("Message Handler ready")

	return mh
}

func (mh *messageHandler) GetReports() chan Report {
	reports := make(chan Report)

	go func() {
		for message := range mh.out {
			reports <- message.GetReport()
		}

		close(reports)
	}()

	return reports
}

func (mh *messageHandler) GetOrderedReports() chan Report {
	reports := make(chan Report)

	go func() {
		for message := range mh.out {
			report := message.GetReport()

			if message.seq == mh.currentReport {
				// The message is the expected. Send it.
				reports <- report
				mh.currentReport++
			} else {
				// This message is not the expected. Store it.
				mh.queued[message.seq] = report
			}

			// Check if there are stored messages and send them.
			for {
				if currentReport, ok := mh.queued[mh.currentReport]; ok {
					reports <- currentReport
					delete(mh.queued, mh.currentReport)
					mh.currentReport++
				} else {
					break
				}
			}
		}

		close(reports)
	}()

	return reports
}
