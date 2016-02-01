[![Build Status](https://travis-ci.org/Bigomby/go-pipes.svg?branch=master)](https://travis-ci.org/Bigomby/go-pipes)
[![Coverage Status](https://coveralls.io/repos/Bigomby/go-pipes/badge.svg?branch=master&service=github)](https://coveralls.io/github/Bigomby/go-pipes?branch=master)

# go-pipes

**go-pipes** is a multi-protocol data forwarder. You can use it to pipe data between different protocols. The application also support decoding, encoding and processing data, for example it can decode a JSON and performs modifications on it before send it encoded as JSON again.

This application uses a pipeline to route received data. Support multiple workers on each stage, message batching and compression using zlib for better perfomance and bandwidth consumption.

## Components

### Sources

- HTTP
- Kafka
- MQTT

### Outputs

- MQTT 
- HTTP 
- Kafka

### Decoders / Encoders

- JSON

### Processors

- Template

## Road Map

|   Milestone   |                   Feature                       |      Status     |
|:-------------:|:-----------------------------------------------:|:---------------:| 
|     0.1       |             Pipeline structure                  |         0.0%    |
|     0.2       |             Null Decoder                        |         0.0%    |
|     0.2       |             Null Encoder                        |         0.0%    |
|     0.2       |             Null Processor                      |         0.0%    |
|     0.2       |             Stdout Sender                       |         0.0%    |
|     0.3       |             JSON Decoder                        |         0.0%    |
|     0.3       |             JSON Encoder                        |         0.0%    |
|     0.3       |             JSON Template Processor             |         0.0%    |
|     0.4       |             Kafka Consumer                      |         0.0%    |
|     0.5       |             HTTP Sender                         |         0.0%    |
|     0.6       |             MQTT Suscriber                      |         0.0%    |
|     0.6       |             MQTT Publisher                      |         0.0%    |
|     0.7       |             Kafka Producer                      |         0.0%    |
|     0.7       |             HTTP Listener                       |         0.0%    |
|     0.8       |             e2e testing                         |         0.0%    |
|     0.9       |             Benchmarks                          |         0.0%    |
|     1.0       |             InfluxDB metrics                    |         0.0%    |


## Example config file 

This is an example config file for a MQTT to HTTP with json decoding/encoding:

```yaml
listener:
  type: "mqtt"
  config:
    broker: "tcp://localhost:1883"
    topics:
      - "test"
decoder:
  type: "json"
processor:
  type: "template"
  config:
    template: examples/template.json
encoder:
  type: "json"
sender:
  type: "http"
  config:
    url: "http://localhost:8080"
    batchsize: 100
    batchtimeout: 500
    deflate: true
```
