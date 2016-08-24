// +nobuild examples

package main

import (
	"fmt"
	"sync"

	"github.com/redBorder/rbforwarder"
	"github.com/redBorder/rbforwarder/components/batch"
	"github.com/redBorder/rbforwarder/components/httpsender"
)

func main() {
	var wg sync.WaitGroup
	var components []interface{}
	const numMessages = 100000

	f := rbforwarder.NewRBForwarder(rbforwarder.Config{
		Retries:   1,
		Backoff:   1,
		QueueSize: 1000,
	})

	batch := &batcher.Batcher{
		Config: batcher.Config{
			Workers:       2,
			TimeoutMillis: 100,
			Limit:         10000,
		},
	}
	components = append(components, batch)

	sender := &httpsender.HTTPSender{
		Config: httpsender.Config{
			Workers: 10,
			URL:     "http://localhost:8888",
		},
	}
	components = append(components, sender)

	f.PushComponents(components)

	opts := map[string]interface{}{
		"http_endpoint": "test",
		"batch_group":   "example",
	}

	f.Run()

	wg.Add(1)
	go func() {
		var errors int
		var messages int

		fmt.Print("[")
		for report := range f.GetReports() {
			r := report.(rbforwarder.Report)
			// fmt.Printf("[MESSAGE %d] %s\n", r.Opaque.(int), r.Status)
			messages++
			if messages%(numMessages/20) == 0 {
				fmt.Printf("=")
			}
			if r.Code > 0 {
				errors += r.Code
			}
			if messages >= numMessages {
				break
			}
		}
		fmt.Print("] ")
		fmt.Printf("Sent %d messages with %d errors\n", messages, errors)
		wg.Done()
	}()

	for i := 0; i < numMessages; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	wg.Wait()
}
