package servicebots

import "code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/mqtt"

// MQTTClient captures the MQTT operations ServiceBot relies on so we can mock it in tests.
type MQTTClient interface {
	Subscribe(topic string, handler func(topic string, payload []byte)) error
	PublishElection(mqtt.ElectionMessage) error
	PublishAnswer(mqtt.AnswerMessage) error
	PublishVictory(mqtt.VictoryMessage) error
	PublishHeartbeat(mqtt.HeartbeatMessage) error
	PublishBotMetadata(mqtt.BotMetadata) error
	PublishPosition(botType string, position mqtt.PositionMessage) error
	PublishStatus(botType string, botID int, status string) error
	PublishTaskEvent(mqtt.TaskAssignmentEvent) error
	Disconnect(waitMillis uint)
}

// Ensure the real MQTT client satisfies the interface.
var _ MQTTClient = (*mqtt.Client)(nil)
