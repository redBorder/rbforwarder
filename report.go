package rbforwarder

import "github.com/oleiade/lane"

type report struct {
	seq     uint64
	code    int
	status  string
	retries int

	opaque *lane.Stack
}
