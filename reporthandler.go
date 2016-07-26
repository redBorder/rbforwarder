package rbforwarder

import (
	"time"

	"github.com/redBorder/rbforwarder/pipeline"
)

// reportHandlerConfig is used to store the configuration for the reportHandler
type reportHandlerConfig struct {
	maxRetries int
	backoff    int
	queueSize  int
}

// reportHandler is used to handle the reports produced by the last element
// of the pipeline. The first element of the pipeline can know the status
// of the produced message using GetReports() or GetOrderedReports()
type reportHandler struct {
	in            chan *pipeline.Message     // Receive messages
	freedMessages chan *pipeline.Message     // Messages after its report has been delivered
	retry         chan *pipeline.Message     // Send messages to retry
	unordered     chan pipeline.Report       // Send reports out of order
	out           chan pipeline.Report       // Send reports in order
	close         chan struct{}              // Stop sending reports
	queued        map[uint64]pipeline.Report // Store pending reports
	currentReport uint64                     // Last delivered report

	config reportHandlerConfig
}

// newReportHandler creates a new instance of reportHandler
func newReportHandler(maxRetries, backoff, queueSize int, retry chan *pipeline.Message) *reportHandler {
	return &reportHandler{
		in:            make(chan *pipeline.Message, queueSize),
		freedMessages: make(chan *pipeline.Message, queueSize),
		retry:         retry,
		unordered:     make(chan pipeline.Report, queueSize),
		close:         make(chan struct{}),
		queued:        make(map[uint64]pipeline.Report),
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
				if message.Report.StatusCode == 0 ||
					r.config.maxRetries == 0 ||
					(r.config.maxRetries > 0 &&
						message.Report.Retries >= r.config.maxRetries) {

					// Create a copy of the report
					report := message.Report

					// Reset message data
					message.OutputBuffer.Reset()
					message.Data = nil
					message.Report = pipeline.Report{}

					// Send back the message to the pool
					r.freedMessages <- message

					// Send the report to the client
					r.unordered <- report
				} else {
					// Retry in other case
					go func() {
						message.Report.Retries++
						Logger.
							WithField("ID", message.Report.ID).
							WithField("Retry", message.Report.Retries).
							WithField("Status", message.Report.Status).
							WithField("Code", message.Report.StatusCode).
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

func (r *reportHandler) GetReports() chan pipeline.Report {
	done := make(chan struct{})
	results := make(chan pipeline.Report)

	go func() {
		done <- struct{}{}

		for report := range r.unordered {
			results <- report
		}
		close(results)
	}()

	<-done
	return results
}

func (r *reportHandler) GetOrderedReports() chan pipeline.Report {
	done := make(chan struct{})
	results := make(chan pipeline.Report)

	go func() {
		done <- struct{}{}
		for report := range r.unordered {
			if report.ID == r.currentReport {
				// The message is the expected. Send it.
				results <- report
				r.currentReport++
			} else {
				// This message is not the expected. Store it.
				r.queued[report.ID] = report
			}

			// Check if there are stored messages and send them.
			for {
				if currentReport, ok := r.queued[r.currentReport]; ok {
					results <- currentReport
					delete(r.queued, r.currentReport)
					r.currentReport++
				} else {
					break
				}
			}
		}
		close(results)
	}()

	<-done
	return results
}
