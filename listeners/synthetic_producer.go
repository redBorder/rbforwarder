package listeners

import (
	"bytes"
	"encoding/json"
	"math/rand"
	// "time"

	"github.com/redBorder/rb-forwarder/util"
)

type Config struct {
	Topic  string
	Fields interface{}
}

type Flow struct {
	Client_latlong      string `json:"client_latlong"`
	Dst_country_code    string `json:"dst_country_code"`
	Dot11_status        string `json:"dot11_status"`
	Bytes               int64  `json:"bytes"`
	Src_net_name        string `json:"src_net_name"`
	Flow_sampler_id     int    `json:"flow_sampler_id"`
	Direction           string `json:"direction"`
	Wireless_station    string `json:"wireless_station"`
	Biflow_direction    string `json:"biflow_direction"`
	Pkts                int64  `json:"pkts"`
	Dst                 string `json:"dst"`
	Campus              string `json:"campus"`
	Building            string `json:"building"`
	Floor               string `json:"floor"`
	Timestamp           int64  `json:"timestamp"`
	Client_mac          string `json:"client_mac"`
	Wireless_id         string `json:"wireless_id"`
	Flow_end_reason     string `json:"flow_end_reason"`
	Src_net             string `json:"src_net"`
	Client_rssi_num     int    `json:"client_rssi_num"`
	Engine_id_name      string `json:"engine_id_name"`
	Src                 string `json:"src"`
	Application_id      string `json:"application_id"`
	Sensor_ip           string `json:"sensor_ip"`
	Application_id_name string `json:"application_id_na"`
	Dst_net             string `json:"dst_net"`
	Dst_net_name        string `json:"dst_net_name"`
	L4_proto            int    `json:"l4_proto"`
	Ip_protocol_version int    `json:"ip_protocol_versi"`
	Sensor_name         string `json:"sensor_name"`
	Src_country_code    string `json:"src_country_code "`
	Engine_id           int    `json:"engine_id"`
	Client_mac_vendor   string `json:"client_mac_vendor"`
	First_switched      int64  `json:"first_switched"`
	Http_host           string `json:"http_host"`
	Src_port            int    `json:"src_port"`
	Namespace_uuid      int    `json:"namespace_uuid"`
	Sensor_uuid         int    `json:"sensor_uuid"`
}

type SyntheticProducer struct {
	rawConfig util.Config
	c         chan *util.Message
	alive     bool

	counter int64
}

func (l *SyntheticProducer) Listen() chan *util.Message {
	// Create the message channel
	l.c = make(chan *util.Message)

	// Parse the configuration
	// l.parseConfig()

	l.alive = true

	for i := 0; i < 50; i++ {
		go func() {
			for l.alive {
				config := Config{
					Topic: "rb_flow",
					Fields: Flow{
						Client_latlong:      "37.89,26.39",
						Dst_country_code:    "US",
						Dot11_status:        "PROBING",
						Bytes:               rand.Int63n(90) + 10,
						Src_net_name:        "0.0.0.0/0",
						Flow_sampler_id:     0,
						Direction:           "ingress",
						Wireless_station:    "00:00:00:00:22:FF",
						Biflow_direction:    "initiator",
						Pkts:                rand.Int63n(500),
						Dst:                 "80.150.0.1",
						Campus:              "Campus A",
						Building:            "Building A",
						Floor:               "Floor A",
						Timestamp:           1454669936,
						Client_mac:          "00:00:00:00:00:00",
						Wireless_id:         "SSID_A",
						Flow_end_reason:     "idle timeout",
						Src_net:             "0.0.0.0/0",
						Client_rssi_num:     80,
						Engine_id_name:      "IANA-L4",
						Src:                 "192.168.255.255",
						Application_id:      "11:101",
						Sensor_ip:           "90.1.44.3 ",
						Application_id_name: "CAIlic",
						Dst_net:             "0.0.0.0/0",
						Dst_net_name:        " 0.0.0.0/0",
						L4_proto:            11,
						Ip_protocol_version: 4,
						Sensor_name:         "sensorA",
						Src_country_code:    "US",
						Engine_id:           10,
						Client_mac_vendor:   "SAMSUNG ELECTRO-MECHANICS",
						First_switched:      1454665936,
						Http_host:           "ambisan.es",
						Src_port:            80,
						Namespace_uuid:      2222,
						Sensor_uuid:         4,
					},
				}

				buf, _ := json.Marshal(config.Fields)
				message := &util.Message{
					Attributes: map[string]string{
						"path": config.Topic,
					},
					InputBuffer: bytes.NewBuffer(buf),
				}

				l.c <- message
				// time.Sleep(1 * time.Second)
			}
		}()
	}

	return l.c
}

// parseConfig
func (l *SyntheticProducer) parseConfig() {

}

func (l *SyntheticProducer) Close() {
	l.alive = false
}
