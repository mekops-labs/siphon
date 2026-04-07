package mqtt

import (
	"crypto/tls"
	"fmt"
	"log"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang" // Aliased to paho to avoid package name conflicts
	"github.com/mekops-labs/siphon/internal/utils"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/sink"
	"github.com/mitchellh/mapstructure"
)

type mqttParams struct {
	URL      string `mapstructure:"url"`
	User     string `mapstructure:"user"`
	Pass     string `mapstructure:"pass"`
	Topic    string `mapstructure:"topic"`
	QoS      byte   `mapstructure:"qos"`
	Retained bool   `mapstructure:"retained"`
}

type mqttSink struct {
	params mqttParams
	client paho.Client
}

// Ensure mqttSink implements sink.Sink
var _ sink.Sink = (*mqttSink)(nil)

func init() {
	sink.Registry.Add("mqtt", New)
}

func New(params any, _ bus.Bus) (sink.Sink, error) {
	var opt mqttParams
	if err := mapstructure.Decode(params, &opt); err != nil {
		return nil, fmt.Errorf("failed to decode mqtt sink params: %w", err)
	}

	if opt.URL == "" {
		return nil, fmt.Errorf("mqtt sink requires a url")
	}
	if opt.Topic == "" {
		return nil, fmt.Errorf("mqtt sink requires a topic")
	}

	opts := paho.NewClientOptions()
	opts.AddBroker(opt.URL)
	opts.SetClientID("siphon-sink-" + utils.RandomString(5))

	if opt.User != "" {
		opts.SetUsername(opt.User)
	}
	if opt.Pass != "" {
		opts.SetPassword(opt.Pass)
	}

	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	// Match collector's TLS setup for local SSL brokers if needed
	opts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})

	opts.OnConnect = func(client paho.Client) {
		log.Printf("MQTT Sink connected to broker: %s", opt.URL)
	}
	opts.OnConnectionLost = func(client paho.Client, err error) {
		log.Printf("MQTT Sink connection lost: %v", err)
	}

	client := paho.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(2 * time.Second) {
		return nil, fmt.Errorf("mqtt sink timed out connecting to mqtt")
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqtt sink failed to connect: %w", err)
	}

	return &mqttSink{
		params: opt,
		client: client,
	}, nil
}

func (s *mqttSink) Send(b []byte) error {
	if !s.client.IsConnected() {
		return fmt.Errorf("mqtt sink is not connected to the broker")
	}

	// Publish the raw bytes directly to the topic
	token := s.client.Publish(s.params.Topic, s.params.QoS, s.params.Retained, b)
	token.Wait()

	if token.Error() != nil {
		return fmt.Errorf("mqtt sink publish failed: %w", token.Error())
	}

	return nil
}

func (s *mqttSink) Close() error {
	if s.client != nil && s.client.IsConnected() {
		log.Printf("MQTT Sink: Disconnecting from broker %s", s.params.URL)
		s.client.Disconnect(250) // Wait 250ms to ensure pending messages are sent
	}
	return nil
}
