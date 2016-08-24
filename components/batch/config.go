package batcher

// Config stores the config for a Batcher
type Config struct {
	Workers           int
	Deflate           bool
	TimeoutMillis     uint
	Limit             uint64
	MaxPendingBatches uint
}
