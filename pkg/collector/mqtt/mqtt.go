package mqtt

import (
	"crypto/tls"
	"log"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mekops-labs/siphon/internal/utils"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mitchellh/mapstructure"
)

type mqttSource struct {
	client      mqtt.Client
	mqttOptions *mqtt.ClientOptions
	topics      []string // List of topics requested by the V2 Pipelines
	lock        sync.Mutex
	bus         bus.Bus // Reference to the central Event Bus
}

type MqttParams struct {
	Url  string
	User string
	Pass string
}

// Ensure mqttSource implements the new Collector interface
var _ collector.Collector = (*mqttSource)(nil)

func init() {
	collector.Registry.Add("mqtt", New)
}

func New(p any) collector.Collector {
	var opt MqttParams

	if err := mapstructure.Decode(p, &opt); err != nil {
		return nil
	}

	if opt.Url == "" {
		return nil
	}

	m := &mqttSource{
		topics: make([]string, 0),
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(opt.Url)
	opts.SetClientID("siphon-" + utils.RandomString(5))
	opts.SetUsername(opt.User)
	opts.SetPassword(opt.Pass)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetTLSConfig(&tls.Config{RootCAs: nil})

	// Connection handlers
	opts.OnConnect = func(client mqtt.Client) {
		log.Print("MQTT connected to broker. Subscribing to pipeline topics...")
		m.subscribe()
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		log.Printf("MQTT connection lost: %v", err)
	}

	m.mqttOptions = opts
	m.client = mqtt.NewClient(opts)

	return m
}

func (m *mqttSource) subscribe() {
	if !m.client.IsConnected() || m.bus == nil {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	for _, topic := range m.topics {
		// Subscribe and push raw payloads directly to the Event Bus
		if token := m.client.Subscribe(topic, 0, func(c mqtt.Client, msg mqtt.Message) {

			// The Collector acts as a pure bridge to the Bus
			if err := m.bus.Publish(msg.Topic(), msg.Payload()); err != nil {
				log.Printf("MQTT Bus Publish Error (%s): %v", msg.Topic(), err)
			}

		}); token.Wait() && token.Error() != nil {
			log.Printf("MQTT Subscribe Error (%s): %v", topic, token.Error())
		} else {
			log.Printf("MQTT Subscribed to topic: %s", topic)
		}
	}
}

// Start connects to the broker and saves the bus reference
func (m *mqttSource) Start(b bus.Bus) {
	m.bus = b
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("MQTT connect error: %v", token.Error())
	}
}

func (m *mqttSource) End() {
	m.client.Disconnect(100)
}

// RegisterTopic replaces AddDataSource. It tells the collector what to listen for based on pipeline configs.
func (m *mqttSource) RegisterTopic(topic string) {
	m.lock.Lock()
	m.topics = append(m.topics, topic)
	m.lock.Unlock()

	// If already connected (e.g., config reload), subscribe immediately
	if m.client.IsConnected() {
		m.subscribe()
	}
}
