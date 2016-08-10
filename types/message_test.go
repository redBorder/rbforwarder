package types

// import . "github.com/smartystreets/goconvey/convey"

// func TestMessage(t *testing.T) {
// 	Convey("Given a payload and some metadata", t, func() {
// 		payload := "This is the payload"
// 		metadata := map[string]interface{}{
// 			"key1": "value1",
// 			"key2": "value2",
// 		}
//
// 		Convey("When the data is stored in a message", func() {
// 			m := new(Message)
// 			m.Payload = lane.NewStack()
// 			m.Opts = lane.NewStack()
//
// 			m.Payload.Push([]byte(payload))
// 			m.Opts.Push(metadata)
//
// 			Convey("Then the data can be recovered through messenger methods", func() {
// 				data := m.Payload.Pop().([]byte)
// 				opts := m.Opts.Pop().(map[string]interface{})
//
// 				So(string(data), ShouldEqual, payload)
// 				So(opts, ShouldNotBeNil)
//
// 				So(opts["key1"], ShouldEqual, "value1")
// 				So(opts["key2"], ShouldEqual, "value2")
// 			})
// 		})
// 	})
//
// 	Convey("Given a message with no options or no data", t, func() {
// 		m := &Message{
// 			Payload: lane.NewStack(),
// 			Opts:    lane.NewStack(),
// 		}
//
// 		Convey("When trying to get message data", func() {
// 			_, dataOk := m.Payload.Pop().([]byte)
// 			_, optsOk := m.Opts.Pop().(map[string]interface{})
//
// 			Convey("Then should error", func() {
// 				So(dataOk, ShouldBeFalse)
// 				So(optsOk, ShouldBeFalse)
// 			})
// 		})
// 	})
//
// 	Convey("Given a delivered message", t, func() {
// 		m := &Message{
// 			Payload: lane.NewStack(),
// 			Opts:    lane.NewStack(),
// 			Reports: lane.NewStack(),
// 		}
//
// 		m.Reports.Push("This is a report")
// 		m.Reports.Push("This is a report")
// 		m.Reports.Push("This is a report")
// 		m.Payload.Push([]byte("This message is delivered"))
// 		m.Opts.Push(map[string]interface{}{
// 			"key1": "value1",
// 			"key2": "value2",
// 		})
//
// 		Convey("Then a report can be obtained for the message", func() {
// 			var i int
// 			for !m.Reports.Empty() {
// 				i++
// 				rep := m.Reports.Pop().(string)
// 				So(rep, ShouldEqual, "This is a report")
// 			}
// 			So(i, ShouldEqual, 3)
// 		})
// 	})
// }
