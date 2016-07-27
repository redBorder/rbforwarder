package pipeline

// Processor performs operations on a data structure
type Processor interface {
	Init(int) error
	Process(Messenger) (bool, error)
}
