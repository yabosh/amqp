package amqp

import (
	"fmt"
)

// BrokerConfig contains the fundamental configuration values required to connect
// to an AMQP host.
type BrokerConfig struct {
	Host              string
	Port              string
	User              string
	Password          string
	ConsumerName      string
	ExchangeName      string
	ConnectionTimeout string
}

// url generates an amqp url that is used internally to connect to a configured
// AMQP host.
func (mb BrokerConfig) url() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s", mb.User, mb.Password, mb.Host, mb.Port)
}
