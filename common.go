package amqp

import (
	"github.com/streadway/amqp"
)

// ensureExchangeExists will verify if the specified exchange exists and, if not, it will create it.
func ensureExchangeExists(channel *amqp.Channel, exchangeName string, exchangeType string) error {
	return channel.ExchangeDeclare(
		exchangeName, // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // argument
	)
}
