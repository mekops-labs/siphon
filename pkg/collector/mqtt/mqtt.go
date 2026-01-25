package mqtt

import (
	"crypto/tls"
	"log"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mekops-labs/siphon/internal/utils"
	"github.com/mekops-labs/siphon/pkg/collector"
	"github.com/mekops-labs/siphon/pkg/parser"
	"github.com/mitchellh/mapstructure"
)

type subscription struct {
	topic  string
	parser *parser.Parser
	active bool
}

type mqttSource struct {
	client             mqtt.Client
	mqttOptions        *mqtt.ClientOptions
	subscriptions      []subscription
	lock               sync.Mutex
	connectHandler     mqtt.OnConnectHandler
	connectLostHandler mqtt.ConnectionLostHandler
}

type MqttParams struct {
	Url  string
	User string
	Pass string
}

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

	ssl := tls.Config{
		RootCAs: nil,
	}

	m := &mqttSource{}
	m.subscriptions = make([]subscription, 0)
	m.connectHandler = func(client mqtt.Client) {
		log.Print("MQTT connected to broker. Sending subscription requests...")
		m.subscribe()
	}
	m.connectLostHandler = func(client mqtt.Client, err error) {
		log.Printf("MQTT connection lost: %v", err)
		for i := range m.subscriptions {
			m.subscriptions[i].active = false
		}
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(opt.Url)
	opts.SetClientID("siphon-" + utils.RandomString(5))
	opts.SetUsername(opt.User)
	opts.SetPassword(opt.Pass)
	opts.OnConnect = m.connectHandler
	opts.OnConnectionLost = m.connectLostHandler
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetTLSConfig(&ssl)

	client := mqtt.NewClient(opts)

	m.mqttOptions = opts
	m.client = client

	return m
}

func (m *mqttSource) connect() error {
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (m *mqttSource) subscribe() {
	if !m.client.IsConnected() {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	for i, s := range m.subscriptions {
		if !s.active {
			p := *m.subscriptions[i].parser
			if token := m.client.Subscribe(s.topic, 0, func(c mqtt.Client, msg mqtt.Message) {
				if err := p.Parse(msg.Payload()); err != nil {
					log.Printf("MQTT: %s: can't parse message: %s", msg.Topic(), err)
				}
			}); token.Wait() && token.Error() != nil {
				log.Print(token.Error())
				continue
			}
			m.subscriptions[i].active = true
		}
	}
}

func (m *mqttSource) End() {
	m.client.Disconnect(100)
}

func (m *mqttSource) Start() error {
	return m.connect()
}

func (m *mqttSource) AddDataSource(topic string, parser parser.Parser) error {
	s := subscription{
		topic:  topic,
		parser: &parser,
	}
	m.subscriptions = append(m.subscriptions, s)
	m.subscribe()
	return nil
}
