package types

import "github.com/oleiade/lane"

// Message is used to send data through the pipeline
type Message struct {
	Payload *lane.Stack
	Opts    *lane.Stack
	Reports *lane.Stack
}

// // PushData store data on an LIFO queue so the nexts handlers can use it
// func (m *Message) PushData(v []byte) {
// 	if m.Payload == nil {
// 		m.Payload = lane.NewStack()
// 	}
//
// 	m.Payload.Push(v)
// }
//
// // PopData get the data stored by the previous handler
// func (m *Message) PopData() (ret []byte, err error) {
// 	if m.Payload == nil {
// 		err = errors.New("Uninitialized payload")
// 		return
// 	}
//
// 	if m.Payload.Empty() {
// 		err = errors.New("No data")
// 		return
// 	}
//
// 	ret = m.Payload.Pop().([]byte)
// 	return
// }
//
// // PopData get the data stored by the previous handler
// func (m *Message) PopOpts() (ret map[string]interface{}, err error) {
// 	if m.Opts == nil {
// 		err = errors.New("Uninitialized options")
// 		return
// 	}
//
// 	if m.Opts.Empty() {
// 		err = errors.New("No options")
// 		return
// 	}
//
// 	ret = m.Opts.Pop().(map[string]interface{})
// 	return
// }
//
// func (m *Message) PushOpts(Opts map[string]interface{}) error {
// 	if m.Opts == nil {
// 		return errors.New("Uninitialized options")
// 	}
//
// 	m.Opts.Push(Opts)
// 	return nil
// }
