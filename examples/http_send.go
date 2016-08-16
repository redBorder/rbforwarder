package main

import (
	"fmt"

	"github.com/redBorder/rbforwarder"
	"github.com/redBorder/rbforwarder/components/batch"
	"github.com/redBorder/rbforwarder/components/httpsender"
)

func main() {
	var components []interface{}
	var workers []int
	const numMessages = 100000

	f := rbforwarder.NewRBForwarder(rbforwarder.Config{
		Retries:   3,
		Backoff:   5,
		QueueSize: 10000,
	})

	batch := &batcher.Batcher{
		Config: batcher.Config{
			TimeoutMillis: 1000,
			Limit:         1000,
		},
	}
	components = append(components, batch)
	workers = append(workers, 1)

	sender := &httpsender.HTTPSender{
		URL: "http://localhost:8888",
	}
	components = append(components, sender)
	workers = append(workers, 1)

	f.PushComponents(components, workers)

	opts := map[string]interface{}{
		"http_endpoint": "librb-http",
		"batch_group":   "librb-http",
	}

	for i := 0; i < numMessages; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	var errors int
	for report := range f.GetReports() {
		r := report.(rbforwarder.Report)
		if r.Code > 0 {
			errors += r.Code
		}
		if r.Opaque.(int) == numMessages-1 {
			break
		}
	}

	fmt.Printf("Sent %d messages with %d errors\n", numMessages, errors)
}
