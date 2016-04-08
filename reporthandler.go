package rbforwarder

import "time"

// reportHandlerConfig is used to store the configuration for the reportHandler
type reportHandlerConfig struct {
	maxRetries int
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
	queued        map[uint64]Report // Used to store pending reports
	currentReport uint64            // Last delivered report

	config reportHandlerConfig
}

// newReportHandler creates a new instance of reportHandler
func newReportHandler(maxRetries int) *reportHandler {
	return &reportHandler{
		in:            make(chan *Message),
		retries:       make(chan *Message),
		freedMessages: make(chan *Message),
		unordered:     make(chan Report),
		out:           make(chan Report),
		queued:        make(map[uint64]Report),
		config: reportHandlerConfig{
			maxRetries: maxRetries,
		},
	}
}

// Init initializes the processing of reports
func (r *reportHandler) Init() {
	go func() {
		// Get reports from the input channel
		for message := range r.in {
			if message.report.StatusCode == 0 {
				message.InputBuffer.Reset()
				message.OutputBuffer.Reset()
				message.Data = nil
				message.Metadata = make(map[string]interface{})

				// Send to the source a copy of the report
				report := message.report
				message.report = Report{}

				// Send back the message to the pool
				select {
				case r.freedMessages <- message:
				case <-time.After(1 * time.Second):
					logger.Error("Can't put back the message on the pool")
				}

				// Send the report to the orderer
			forLoop:
				for {
					select {
					case r.unordered <- report:
						break forLoop
					case <-time.After(100 * time.Millisecond):
						logger.Error("Error on report: Full queue")
					}
				}
			} else {

				// Fail

				// Retry this message
				if r.config.maxRetries < 0 ||
					message.report.Retries < r.config.maxRetries {

					message.report.Retries++
					logger.Warnf("Retrying message: %d | Reason: %s",
						message.report.ID, message.report.Status)
					if err := message.Produce(); err != nil {
						logger.Error(err)
					}
				} else {

					// Give up

					// Clean the message before send it to the message pool
					message.InputBuffer.Reset()
					message.OutputBuffer.Reset()
					message.Data = nil
					message.Metadata = make(map[string]interface{})

					// Send back the message to the pool
					select {
					case r.freedMessages <- message:
					default:
						logger.Error("Can't put back the message on the pool")
					}

					// Send to the source a copy of the report
					report := message.report
					message.report = Report{}

					select {
					case r.unordered <- report:
					default:
						logger.Error("Error on report: Full queue")
					}
				}
			}
		}
	}()
}

func (r *reportHandler) GetReports() chan Report {
	done := make(chan struct{})

	go func() {
		done <- struct{}{}

		for report := range r.unordered {
			r.out <- report
		}
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
	}()
	<-done
	return r.out
}
