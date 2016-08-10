package batcher

// Config stores the config for a Batcher
type Config struct {
	TimeoutMillis     uint
	Limit             uint
	MaxPendingBatches uint
}
