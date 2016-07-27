package rbforwarder

import "time"

// reportHandler is used to handle the reports produced by the last element
// of the pipeline. The first element of the pipeline can know the status
// of the produced message using GetReports() or GetOrderedReports()
type messageHandler struct {
	input         chan *message     // Receive messages
	freedMessages chan *message     // Messages after its report has been delivered
	retry         chan *message     // Send messages to retry
	unordered     chan *message     // Send reports out of order
	out           chan *message     // Send reports in order
	close         chan struct{}     // Stop sending reports
	queued        map[uint64]Report // Store pending reports
	currentReport uint64            // Last delivered report

	config messageHandlerConfig
}

// newReportHandler creates a new instance of reportHandler
func newMessageHandler(maxRetries, backoff, queueSize int,
	retry chan *message) *messageHandler {

	return &messageHandler{
		input:         make(chan *message, queueSize),
		freedMessages: make(chan *message, queueSize),
		unordered:     make(chan *message, queueSize),
		close:         make(chan struct{}),
		queued:        make(map[uint64]Report),
		retry:         retry,
		config: messageHandlerConfig{
			maxRetries: maxRetries,
			backoff:    backoff,
		},
	}
}

// Init initializes the processing of reports
func (r *messageHandler) Init() {
	go func() {
		// Get reports from the input channel
	forOuterLoop:
		for {
			select {
			case <-r.close:
				break forOuterLoop
			case message := <-r.input:
				// Report when:
				// - Message has been received successfully
				// - Retrying has been disabled
				// - The max number of retries has been reached
				if message.code == 0 ||
					r.config.maxRetries == 0 ||
					(r.config.maxRetries > 0 &&
						message.retries >= r.config.maxRetries) {

					// Send the report to the client
					r.unordered <- message
				} else {
					// Retry in other case
					go func() {
						message.retries++
						Logger.
							WithField("Seq", message.seq).
							WithField("Retry", message.retries).
							WithField("Status", message.status).
							WithField("Code", message.code).
							Warnf("Retrying message")

						<-time.After(time.Duration(r.config.backoff) * time.Second)
						r.retry <- message
					}()
				}
			}
		}
		close(r.unordered)
	}()

	Logger.Debug("Report Handler ready")
}

func (r *messageHandler) GetReports() chan Report {
	done := make(chan struct{})
	reports := make(chan Report)

	go func() {
		done <- struct{}{}

		for message := range r.unordered {
			reports <- message.GetReport()
		}
		close(reports)
	}()

	<-done
	return reports
}

func (r *messageHandler) GetOrderedReports() chan Report {
	done := make(chan struct{})
	reports := make(chan Report)

	go func() {
		done <- struct{}{}
		for message := range r.unordered {
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

	<-done
	return reports
}
