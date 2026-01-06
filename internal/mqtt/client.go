package mqtt

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Client wraps the Paho MQTT client with convenience methods
type Client struct {
	client   mqtt.Client
	clientID string
}

// Config holds MQTT client configuration
type Config struct {
	BrokerURL string
	ClientID  string
	Username  string
	Password  string
	// Last Will Testament configuration
	WillEnabled bool
	WillTopic   string
	WillPayload string
	WillQoS     byte
	WillRetain  bool
}

// NewClient creates and connects a new MQTT client with the given configuration
func NewClient(config Config) (*Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.BrokerURL)
	opts.SetClientID(config.ClientID)

	if config.Username != "" {
		opts.SetUsername(config.Username)
		opts.SetPassword(config.Password)
	}

	// Configure Last Will Testament if enabled
	if config.WillEnabled {
		opts.SetWill(config.WillTopic, config.WillPayload, config.WillQoS, config.WillRetain)
	}

	// Connection settings
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetConnectTimeout(10 * time.Second)

	// Auto-reconnect configuration
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(10 * time.Second)

	// Connection lifecycle handlers
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Printf("[MQTT] Connected to broker: %s (client: %s)", config.BrokerURL, config.ClientID)
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("[MQTT] Connection lost: %v (client: %s) - will auto-reconnect", err, config.ClientID)
	})

	opts.SetReconnectingHandler(func(c mqtt.Client, opts *mqtt.ClientOptions) {
		log.Printf("[MQTT] Attempting to reconnect... (client: %s)", config.ClientID)
	})

	// Create and connect
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	return &Client{
		client:   client,
		clientID: config.ClientID,
	}, nil
}

// Publish sends a message to the specified topic with QoS 1
func (c *Client) Publish(topic string, payload interface{}) error {
	var data []byte
	var err error

	// Serialize payload based on type
	switch v := payload.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		// Assume JSON-serializable struct
		data, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %w", err)
		}
	}

	// Publish with QoS 1 (at-least-once delivery)
	token := c.client.Publish(topic, 1, false, data)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish to topic %s: %w", topic, token.Error())
	}

	return nil
}

// Subscribe subscribes to a topic with a message handler callback
// The handler receives the topic and payload for each message
func (c *Client) Subscribe(topic string, handler func(topic string, payload []byte)) error {
	callback := func(client mqtt.Client, msg mqtt.Message) {
		handler(msg.Topic(), msg.Payload())
	}

	// Subscribe with QoS 1
	token := c.client.Subscribe(topic, 1, callback)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, token.Error())
	}

	log.Printf("[MQTT] Subscribed to topic: %s (client: %s)", topic, c.clientID)
	return nil
}

// Unsubscribe removes a subscription from a topic
func (c *Client) Unsubscribe(topic string) error {
	token := c.client.Unsubscribe(topic)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to unsubscribe from topic %s: %w", topic, token.Error())
	}

	log.Printf("[MQTT] Unsubscribed from topic: %s (client: %s)", topic, c.clientID)
	return nil
}

// Disconnect gracefully disconnects from the broker
// waitMillis specifies how long to wait for pending publishes to complete
func (c *Client) Disconnect(waitMillis uint) {
	log.Printf("[MQTT] Disconnecting client: %s", c.clientID)
	c.client.Disconnect(waitMillis)
}

// IsConnected returns the current connection status
func (c *Client) IsConnected() bool {
	return c.client.IsConnected()
}

// PublishStatus is a convenience method to publish bot online/offline status
func (c *Client) PublishStatus(botType string, botID int, status string) error {
	topic := fmt.Sprintf("devices/%s/%d/status", botType, botID)
	return c.Publish(topic, status)
}

// PublishPosition is a convenience method to publish position updates
func (c *Client) PublishPosition(botType string, position PositionMessage) error {
	topic := fmt.Sprintf("devices/%s/%d/position", botType, position.ID)
	return c.Publish(topic, position)
}

// PublishEvent is a convenience method to publish problem events
func (c *Client) PublishEvent(event EventMessage) error {
	return c.Publish("events/problems", event)
}

// PositionMessage represents a position update payload
type PositionMessage struct {
	ID        int    `json:"id"`
	X         int    `json:"x"`
	Y         int    `json:"y"`
	Timestamp string `json:"timestamp"`
}

// EventMessage represents a problem event payload
type EventMessage struct {
	DetectorID int    `json:"detectorId"`
	Type       string `json:"type"`
	X          int    `json:"x"`
	Y          int    `json:"y"`
	Timestamp  string `json:"timestamp"`
}
