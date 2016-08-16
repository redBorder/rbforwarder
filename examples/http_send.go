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

	f := rbforwarder.NewRBForwarder(rbforwarder.Config{
		Retries:   3,
		Backoff:   5,
		QueueSize: 10000,
	})

	batch := &batcher.Batcher{
		Config: batcher.Config{
			TimeoutMillis: 1000,
			Limit:         2,
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

	for i := 0; i < 10; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	for report := range f.GetReports() {
		r := report.(rbforwarder.Report)
		// fmt.Printf("MESSAGE: %d\n", r.Opaque.(int))
		// fmt.Printf("CODE: %d\n", r.Code)
		// fmt.Printf("STATUS: %s\n", r.Status)
		if r.Opaque.(int) == 9999 {
			break
		}
	}
}
