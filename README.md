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
