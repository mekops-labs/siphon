package hass

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/mekops-labs/siphon/internal/utils"
	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/sink"
	"github.com/mitchellh/mapstructure"
)

type hassParams struct {
	URL  string `mapstructure:"url"`
	User string `mapstructure:"user"`
	Pass string `mapstructure:"pass"`

	// Home Assistant Discovery specifics
	DiscoveryPrefix   string `mapstructure:"discovery_prefix"`
	Component         string `mapstructure:"component"`
	NodeID            string `mapstructure:"node_id"`
	ObjectID          string `mapstructure:"object_id"`
	Name              string `mapstructure:"name"`
	DeviceClass       string `mapstructure:"device_class"`
	StateClass        string `mapstructure:"state_class"`
	UnitOfMeasurement string `mapstructure:"unit_of_measurement"`
	ValueTemplate     string `mapstructure:"value_template"`
	Icon              string `mapstructure:"icon"`
	AvailabilityTopic string `mapstructure:"availability_topic"`
}

type hassSink struct {
	params            hassParams
	client            paho.Client
	stateTopic        string
	availabilityTopic string
}

// Ensure hassSink implements sink.Sink
var _ sink.Sink = (*hassSink)(nil)

func init() {
	sink.Registry.Add("hass", New)
}

func New(params any, _ bus.Bus) (sink.Sink, error) {
	var opt hassParams
	if err := mapstructure.Decode(params, &opt); err != nil {
		return nil, fmt.Errorf("failed to decode hass sink params: %w", err)
	}

	// Apply Defaults
	if opt.URL == "" {
		return nil, fmt.Errorf("hass sink requires an mqtt url")
	}
	if opt.ObjectID == "" {
		return nil, fmt.Errorf("hass sink requires an object_id")
	}
	if opt.DiscoveryPrefix == "" {
		opt.DiscoveryPrefix = "homeassistant"
	}
	if opt.Component == "" {
		opt.Component = "sensor"
	}
	if opt.Name == "" {
		opt.Name = "Siphon " + strings.Title(strings.ReplaceAll(opt.ObjectID, "_", " "))
	}

	// Construct Topics
	baseTopic := fmt.Sprintf("%s/%s", opt.DiscoveryPrefix, opt.Component)
	if opt.NodeID != "" {
		baseTopic = fmt.Sprintf("%s/%s", baseTopic, opt.NodeID)
	}
	baseTopic = fmt.Sprintf("%s/%s", baseTopic, opt.ObjectID)

	configTopic := baseTopic + "/config"
	stateTopic := baseTopic + "/state"

	// Default to a sub-topic for availability if not explicitly provided
	availabilityTopic := opt.AvailabilityTopic
	if availabilityTopic == "" {
		availabilityTopic = baseTopic + "/availability"
	}

	// Build the Discovery Config Payload
	configPayload := map[string]interface{}{
		"name":                  opt.Name,
		"state_topic":           stateTopic,
		"availability_topic":    availabilityTopic,
		"payload_available":     "online",
		"payload_not_available": "offline",
		"unique_id":             fmt.Sprintf("siphon_%s", opt.ObjectID),
		"device": map[string]interface{}{
			"identifiers":  []string{"siphon_etl_engine"},
			"name":         "Siphon ETL Engine",
			"manufacturer": "Mekops Labs",
			"model":        "Siphon V2",
		},
	}

	// Add optional fields
	if opt.DeviceClass != "" {
		configPayload["device_class"] = opt.DeviceClass
	}
	if opt.StateClass != "" {
		configPayload["state_class"] = opt.StateClass
	}
	if opt.UnitOfMeasurement != "" {
		configPayload["unit_of_measurement"] = opt.UnitOfMeasurement
	}
	if opt.ValueTemplate != "" {
		configPayload["value_template"] = opt.ValueTemplate
	}
	if opt.Icon != "" {
		configPayload["icon"] = opt.Icon
	}

	configBytes, err := json.Marshal(configPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal hass discovery config: %w", err)
	}

	// Setup MQTT Client
	opts := paho.NewClientOptions()
	opts.AddBroker(opt.URL)
	opts.SetClientID("siphon-hass-sink-" + utils.RandomString(5))

	if opt.User != "" {
		opts.SetUsername(opt.User)
	}
	if opt.Pass != "" {
		opts.SetPassword(opt.Pass)
	}

	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})

	opts.SetWill(availabilityTopic, "offline", 1, true)

	// OnConnect Callback
	opts.OnConnect = func(client paho.Client) {
		log.Printf("HASS Sink connected. Publishing auto-discovery for '%s'", opt.ObjectID)

		// 1. Publish Discovery Config
		client.Publish(configTopic, 1, true, configBytes)

		// 2. Publish Birth Message (Online)
		client.Publish(availabilityTopic, 1, true, []byte("online"))
	}

	opts.OnConnectionLost = func(client paho.Client, err error) {
		log.Printf("HASS Sink MQTT connection lost: %v", err)
	}

	client := paho.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(2 * time.Second) {
		return nil, fmt.Errorf("hass sink timed out connecting to mqtt")
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("hass sink failed to connect to mqtt: %w", err)
	}

	return &hassSink{
		params:            opt,
		client:            client,
		stateTopic:        stateTopic,
		availabilityTopic: availabilityTopic,
	}, nil
}

func (s *hassSink) Send(b []byte) error {
	if !s.client.IsConnected() {
		return fmt.Errorf("hass sink is not connected to the broker")
	}

	// Optional: You could update the availability topic here just to be safe,
	// but the OnConnect birth message combined with LWT usually handles this perfectly.

	token := s.client.Publish(s.stateTopic, 0, false, b)
	token.Wait()

	if token.Error() != nil {
		return fmt.Errorf("hass sink publish failed: %w", token.Error())
	}

	return nil
}

func (s *hassSink) Close() error {
	if s.client != nil && s.client.IsConnected() {
		log.Printf("HASS Sink [%s]: Closing connection and sending offline message", s.params.ObjectID)
		// Explicitly publish offline message so HA updates immediately
		s.client.Publish(s.availabilityTopic, 1, true, "offline").Wait()
		s.client.Disconnect(250)
	}
	return nil
}
