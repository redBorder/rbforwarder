package utils

// Next should be called by a component in order to pass the message to the next
// component in the pipeline.
type Next func()

// Done should be called by a component in order to return the message to the
// message handler. Can be used by the last component to inform that the
// message processing is done o by a middle component to inform an error.
type Done func(*Message, int, string)

// Composer represents a component in the pipeline that performs a work on
// a message
type Composer interface {
	Spawn(int) Composer
	OnMessage(*Message, Done)
	Workers() int
}
