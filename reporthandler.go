package rbforwarder

import "time"

// Report is used by the source to obtain the status of a sent message
type Report struct {
	ID         uint64 // Unique ID for the report, used to maintain sequence
	Status     string // Result of the sending
	StatusCode int    // Result of the sending
	Retries    int
	Metadata   map[string]interface{}
}

// reportHandlerConfig is used to store the configuration for the reportHandler
type reportHandlerConfig struct {
	maxRetries int
	backoff    int
	queue      int
}

// reportHandler is used to handle the reports produced by the last element
// of the pipeline. The first element of the pipeline can know the status
// of the produced message using GetReports() or GetOrderedReports()
type reportHandler struct {
	in            chan *Message     // Used to receive messages
	retries       chan *Message     // Used to send messages if status code is not 0
	freedMessages chan *Message     // Used to send messages messages after its report has been delivered
	unordered     chan Report       // Used to send reports out of order
	out           chan Report       // Used to send reports in order
	close         chan struct{}     // Used to stop sending reports
	queued        map[uint64]Report // Used to store pending reports
	currentReport uint64            // Last delivered report

	config reportHandlerConfig
}

// newReportHandler creates a new instance of reportHandler
func newReportHandler(maxRetries, backoff, queue int) *reportHandler {
	return &reportHandler{
		in:            make(chan *Message, queue),
		retries:       make(chan *Message, queue),
		freedMessages: make(chan *Message, queue),
		unordered:     make(chan Report, queue),
		out:           make(chan Report, queue),
		close:         make(chan struct{}),
		queued:        make(map[uint64]Report),
		config: reportHandlerConfig{
			maxRetries: maxRetries,
			backoff:    backoff,
		},
	}
}

// Init initializes the processing of reports
func (r *reportHandler) Init() {
	go func() {
		// Get reports from the input channel

	forOuterLoop:
		for {
			select {
			case <-r.close:
				break forOuterLoop
			case message := <-r.in:

				// Report when:
				// - Message has been received successfully
				// - Retrying has been disabled
				// - The max number of retries has been reached
				// Retry in the other case
				if message.report.StatusCode == 0 ||
					r.config.maxRetries == 0 ||
					(r.config.maxRetries > 0 &&
						message.report.Retries >= r.config.maxRetries) {

					// Create a copy of the report
					report := message.report
					report.Metadata = message.Metadata

					// Reset message data
					message.InputBuffer.Reset()
					message.OutputBuffer.Reset()
					message.Data = nil
					message.Metadata = make(map[string]interface{})
					message.report = Report{}

					// Send back the message to the pool
				returnReportLoop:
					for {
						select {
						case r.freedMessages <- message:
							break returnReportLoop
						case <-time.After(1 * time.Second):
							logger.Warn("Can't put back the message on the pool")
						}
					}

					// Send the report to the client
				sendReportLoop:
					for {
						select {
						case r.unordered <- report:
							break sendReportLoop
						case <-time.After(100 * time.Millisecond):
							logger.Warn("Error on report: Full queue")
						}
					}
				} else {
					go func() {
						message.report.Retries++
						logger.
							WithField("ID", message.report.ID).
							WithField("Retry", message.report.Retries).
							WithField("Reason", message.report.Status).
							Warnf("Retrying message")

						<-time.After(time.Duration(r.config.backoff) * time.Second)
						if err := message.Produce(); err != nil {
							logger.Error(err)
						}
					}()
				}
			}
		}
		close(r.unordered)
	}()
}

func (r *reportHandler) GetReports() chan Report {
	done := make(chan struct{})

	go func() {
		done <- struct{}{}

		for report := range r.unordered {
			r.out <- report
		}
		close(r.out)
	}()

	<-done
	return r.out
}

func (r *reportHandler) GetOrderedReports() chan Report {
	done := make(chan struct{})
	go func() {
		done <- struct{}{}
		for report := range r.unordered {
			if report.ID == r.currentReport {
				// The message is the expected. Send it.
				r.out <- report
				r.currentReport++
			} else {
				// This message is not the expected. Store it.
				r.queued[report.ID] = report
			}

			// Check if there are stored messages and send them.
			for {
				if currentReport, ok := r.queued[r.currentReport]; ok {
					r.out <- currentReport
					delete(r.queued, r.currentReport)
					r.currentReport++
				} else {
					break
				}
			}
		}
		close(r.out)
	}()

	<-done
	return r.out
}
