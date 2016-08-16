![](https://img.shields.io/packagist/l/doctrine/orm.svg?maxAge=2592000)
[![](https://travis-ci.org/redBorder/rbforwarder.svg?branch=develop)](https://travis-ci.org/redBorder/rbforwarder)
[![](https://goreportcard.com/badge/github.com/redBorder/rbforwarder)](https://goreportcard.com/report/github.com/redBorder/rbforwarder)
[![](https://coveralls.io/repos/github/redBorder/rbforwarder/badge.svg?branch=develop)](https://coveralls.io/github/redBorder/rbforwarder?branch=develop)
[![](https://godoc.org/github.com/redBorder/rbforwarder?status.svg)](https://godoc.org/github.com/redBorder/rbforwarder)

# rbforwarder

**rbforwarder** is an extensible, protocol agnostic and easy to use tool for
processing data asynchronously. It allows you to create a custom pipeline in
a modular fashion.

For example, you can read data from a Kafka broker and use **rbforwarder** to
build a **pipeline** to decodes the JSON, filter or add fields, encode
the data again to JSON and send it using using multiple protocols `HTTP`,
`MQTT`, `AMQP`, etc. It's easy to write a pipeline **component**.   

## Built-in features

- Support multiple **workers** for each components.
- Support **buffer pooling** for memory recycling.
- Asynchronous report system. Get responses on a separate gorutine.
- Built-in message retrying. The **rbforwarder** can retry messages on fail.
- Instrumentation to have an idea of how it is performing the application.

## Components

- Send data to an endpoint
  - MQTT
  - HTTP
  - Kafka
- Decoders / Encoders
  - JSON
- Utility
  - Limiter

## Road Map

_The application is on hard development, breaking changes expected until 1.0._

|Milestone | Feature             | Status    |
|----------|---------------------|-----------|
| 0.1      | Pipeline builder    | Done      |
| 0.2      | Reporter            | Done      |
| 0.3      | Buffer pool         | _Pending_ |
| 0.4      | Batcher component   | _Pending_ |
| 0.5      | Limiter component   | _Pending_ |
| 0.6      | Instrumentation     | _Pending_ |
| 0.7      | HTTP component      | _Pending_ |
| 0.8      | JSON component      | _Pending_ |
| 0.9      | MQTT component      | _Pending_ |
| 1.0      | Kafka component     | _Pending_ |

## Usage

```go
 // Array of components to use and workers
var components []interface{}
var workers []int

// Create an instance of rbforwarder
f := rbforwarder.NewRBForwarder(rbforwarder.Config{
	Retries:   3,     // Number of retries before give up
	Backoff:   5,     // Time to wait before retry a message
	QueueSize: 10000, // Max messageon queue, produce will block when the queue is full
})

// Create a batcher component and add it to the components array
batch := &batcher.Batcher{
	Config: batcher.Config{
		TimeoutMillis: 1000,
		Limit:         1000,
	},
}
components = append(components, batch)
workers = append(workers, 1)

// Create a http component and add it to the components array
sender := &httpsender.HTTPSender{
	URL: "http://localhost:8888",
}
components = append(components, sender)
workers = append(workers, 1)

// Push the component array and workers to the pipeline
f.PushComponents(components, workers)

opts := map[string]interface{}{
	"http_endpoint": "librb-http",
	"batch_group":   "librb-http",
}

// Produce messages. It won't block until the queue is full.
f.Produce([]byte("{\"message\": 1}"), opts, 1)
f.Produce([]byte("{\"message\": 2}"), opts, 2)
f.Produce([]byte("{\"message\": 3}"), opts, 3)
f.Produce([]byte("{\"message\": 4}"), opts, 4)
f.Produce([]byte("{\"message\": 5}"), opts, 5)

// Read reports. You should do this on a separate gorutine so you make sure
// that you won't block
for report := range f.GetReports() {
	r := report.(rbforwarder.Report)
  if r.Opaque.(int) == numMessages-1 {
		break
	}
}
```
