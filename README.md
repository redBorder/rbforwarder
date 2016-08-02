![](https://img.shields.io/packagist/l/doctrine/orm.svg?maxAge=2592000)
[![](https://travis-ci.org/redBorder/rbforwarder.svg?branch=master)](https://travis-ci.org/redBorder/rbforwarder)
[![](https://goreportcard.com/badge/github.com/redBorder/rbforwarder)](https://goreportcard.com/report/github.com/redBorder/rbforwarder)
[![](https://coveralls.io/repos/github/redBorder/rbforwarder/badge.svg?branch=master)](https://coveralls.io/github/redBorder/rbforwarder?branch=master)
[![](https://godoc.org/github.com/redBorder/rbforwarder?status.svg)](https://godoc.org/github.com/redBorder/rbforwarder)

# rbforwarder

**rbforwarder** is an extensible, protocol agnostic and easy to use tool for
process data. It allows you to create a custom pipeline in a modular way.

For example, you can read data from a Kafka broker and use **rbforwarder** to
build a **pipeline** that decodes the JSON, filter or add some fields, encodes
the data again to JSON and send it using using multiple protocols HTTP, MQTT,
AMQP, etc. It's easy to write a **component** for the pipeline.   

## Features

- Support for multiple workers for every **component**.
- Support **buffer pooling**, for fine-grained memory control.
- Asynchronous report system. Different gorutine for send and receive responses.
- Built-in retry. The **rbforwarder** can retry messages on fail.

## Components

- Send data to an endpoint
  - MQTT
  - HTTP
  - Fast HTTP
  - Kafka
- Decoders / Encoders
  - JSON
- Utility
  - Limiter
  - Metrics

## Road Map

_The application is being on hard developing, breaking changes expected._

|Milestone | Feature             | Status    |
|----------|---------------------|-----------|
| 0.1      | Pipeline            | _Testing_ |
| 0.2      | Reporter            | _Testing_ |
| 0.3      | Limiter             | _Testing_ |
| 0.4      | HTTP component      | _Testing_ |
| 0.5      | MQTT component      | _Pending_ |
| 0.6      | JSON component      | _Pending_ |
| 0.7      | Kafka component     | _Pending_ |
| 0.8      | Metrics component   | _Pending_ |
| 0.9      | Benchmarks          | _Pending_ |
| 1.0      | Stable              | _Pending_ |
