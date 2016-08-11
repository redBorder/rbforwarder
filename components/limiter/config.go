package limiter

// Config stores the config for a Limiter
type Config struct {
	MessageLimit uint64
	BytesLimit   uint64
	Burst        uint64
}
