![](https://img.shields.io/packagist/l/doctrine/orm.svg?maxAge=2592000)
[![](https://travis-ci.org/redBorder/rbforwarder.svg?branch=master)](https://travis-ci.org/redBorder/rbforwarder)
[![](https://goreportcard.com/badge/github.com/redBorder/rbforwarder)](https://goreportcard.com/report/github.com/redBorder/rbforwarder)
[![](https://coveralls.io/repos/github/redBorder/rbforwarder/badge.svg?branch=master)](https://coveralls.io/github/redBorder/rbforwarder?branch=develop)
[![](https://godoc.org/github.com/redBorder/rbforwarder?status.svg)](https://godoc.org/github.com/redBorder/rbforwarder)

# rbforwarder

**rbforwarder** is an extensible tool for processing data asynchronously.
It allows you to create a custom pipeline in a modular fashion.

You can use **rbforwarder** to build a **pipeline** that decodes a JSON, filter
or add fields, encode the data again to JSON and send it using multiple
protocols `HTTP`, `MQTT`, `AMQP`, etc. It's easy to write a pipeline **component**.   

## Built-in features

- Support multiple **workers** for each components.
- Asynchronous report system. Get responses on a separate goroutine.
- Built-in message retrying. **rbforwarder** can retry messages on fail.
- Instrumentation, to have an overview of how the application is performing.

## Components

- Send data to an endpoint
  - MQTT
  - HTTP
  - Kafka
- Decoders / Encoders
  - JSON
- Utility
  - Limiter
  - Batcher (supports deflate)

## Road Map

_The application is on hard development, breaking changes expected until 1.0._

|Milestone | Feature             | Status    |
|----------|---------------------|-----------|
| 0.1      | Pipeline            | Done      |
| 0.2      | Reporter            | Done      |
| 0.3      | HTTP component      | Done      |
| 0.4      | Batcher component   | Done      |
| 0.5      | Limiter component   | Done      |
| 0.6      | Instrumentation     | _Pending_ |
| 0.8      | JSON component      | _Pending_ |
| 0.9      | MQTT component      | _Pending_ |
| 1.0      | Kafka component     | _Pending_ |
