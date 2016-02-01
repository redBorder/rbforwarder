package util

import (
	"bytes"
)

type Config map[string]interface{}

type ElementConfig struct {
	Type   string `yaml:"type"`
	Config Config `yaml:"config"`
}

type PipeConfig struct {
	Listener  ElementConfig
	Decoder   ElementConfig
	Processor ElementConfig
	Encoder   ElementConfig
	Sender    ElementConfig
}

type Message struct {
	InputBuffer  *bytes.Buffer
	Data         interface{}
	OutputBuffer *bytes.Buffer
	Attributes   map[string]string
}
