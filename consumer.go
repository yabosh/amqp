package amqp

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
	"github.com/yabosh/go-clock"
	"github.com/yabosh/logger"
	"github.com/yabosh/recovery"
)

// An AMQP consumption client that uses the Observer pattern to return messages
// that are read from a queue.
//
// Each instance of a client supports reading from a single queue that is attached
// to a single exchange with a single binding key.  If the specified exchange
// does not exist, the client attempts to create it.
//
// As messages are read from an AMQP queue they are passed to a function that is
// provided when the consumer is created.

// Payload represents the contents of a message retrieved from an AMQP queue.
type Payload struct {
	ContentType     string
	ContentEncoding string
	CorrelationId   string
	Timestamp       time.Time
	AppId           string
	Exchange        string
	RoutingKey      string
	Body            []byte
}

// MessageProcessor is a type of function that is called whenever a
// message is received from an AMQP queue.
type MessageProcessor func(message *Payload) error

// Consumer refers to a connection to an AMQP instance that reads messages.
type Consumer struct {
	LastHeartbeat               time.Time
	handler                     MessageProcessor
	config                      *consumerConfig
	broker                      *BrokerConfig
	source                      string
	conn                        *amqp.Connection
	channel                     *amqp.Channel
	queue                       *amqp.Queue
	messages                    <-chan amqp.Delivery
	connectionCloseNotification chan *amqp.Error
	channelCloseNotification    chan *amqp.Error
	heartbeatTicker             *time.Ticker
	heartbeatExpiration         time.Duration
	url                         string
}

// consumerConfig represents configuration information that is unique to connections that consume from an AMQP queue
type consumerConfig struct {
	exchangeName      string
	exchangeType      string
	queueName         string
	queueBindingKey   string
	messageConsumer   string
	lifetime          time.Duration
	connectionTimeout time.Duration
}

// newMessageConsumerConfig returns a new, initialized instance of ConsumerConfig
func newMessageConsumerConfig(brokerConfig BrokerConfig, exchange string, exchangeType string, queue string, bindingKey string) *consumerConfig {
	c := new(consumerConfig)
	c.exchangeName = exchange
	c.exchangeType = exchangeType
	c.queueName = queue
	c.queueBindingKey = bindingKey
	c.messageConsumer = brokerConfig.ConsumerName
	c.lifetime, _ = time.ParseDuration("0s") // 0s = infinite
	c.connectionTimeout, _ = time.ParseDuration(brokerConfig.ConnectionTimeout)
	return c
}

// NewConsumer creates a new, initialized instance of an AMQP message consumer using
// the configuration information supplied in config.  New incoming AMQP messages
// are sent to the handler function provided.
func NewConsumer(broker BrokerConfig, handler MessageProcessor, exchangeType string, queueName string, bindingKey string) *Consumer {
	config := newMessageConsumerConfig(
		broker,              // rabbit connection info
		broker.ExchangeName, // exchange name
		exchangeType,        // exchange type
		queueName,           // queue name
		bindingKey,          // queue binding key
	)

	return &Consumer{
		config:  config,
		broker:  &broker,
		handler: handler,
		url:     broker.url(),
	}
}

// SetSource sets a prefix that is appended to any log messages generated by the consumer
func (c *Consumer) SetSource(source string) {
	c.source = source
}

// Start connects to a previously defined AMQP queue
// and begins consuming messages. All messages consumed are sent to the
// MessageProcessor provided in NewConsumer
//
// This call blocks until a panic occurs.
func (c *Consumer) Start() {
	c.connectWithRetry()
	c.receiveLoop()
}

func (c *Consumer) recover(msg string) {
	if err := recover(); err != nil {
		logger.Error(msg, err)
	}
}

func (c *Consumer) receiveLoop() {
	// defer c.recover("panicked receiving message from broker:")
	c.heartbeatExpiration = hearbeatInterval
	c.heartbeatTicker = time.NewTicker(c.heartbeatExpiration)
	c.writeHeartbeat()
	defer c.heartbeatTicker.Stop()

	for {
		select {
		case msg := <-c.messages:
			logger.Info("Read message from queue")

			if err := c.handler(c.createPayload(&msg)); err == nil {
				msg.Ack(false)
			} else {
				logger.Error("AMQP Read: %s", err.Error())
			}

		case connErr := <-c.connectionCloseNotification:
			// This is necessary because the amqp library will immediately
			// close this channel after posting any error so this
			// case path will always be taken and cause an infinite loop
			c.connectionCloseNotification = make(chan *amqp.Error)
			c.conn = nil
			c.channel = nil

			if connErr != nil {
				logger.Error("[%s] connection closed %#v", c.source, connErr)
			}

			c.connectWithRetry()

		case reason := <-c.channelCloseNotification:
			logger.Trace("[%s] Channel closed %#v", c.source, reason)
			c.channel = nil
			c.channelCloseNotification = make(chan *amqp.Error)
			if !c.conn.IsClosed() {
				logger.Trace("[%s] Closing connection to message broker", c.source)
				// Closing the channel will force a reconnect which will address some issues such as a missing exchange, etc.
				c.conn.Close()
			}

		case <-c.heartbeatTicker.C:
		}
		c.writeHeartbeat()
	}
}

func (c *Consumer) createPayload(msg *amqp.Delivery) *Payload {
	return &Payload{
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		CorrelationId:   msg.CorrelationId,
		Timestamp:       msg.Timestamp,
		AppId:           msg.AppId,
		Exchange:        msg.Exchange,
		RoutingKey:      msg.RoutingKey,
		Body:            msg.Body,
	}
}

// connectWithRetry will continuously try to connect to an AMQP
// host until it succeeds.  An exponential backoff is used
// if the connection fails.
func (c *Consumer) connectWithRetry() {
	var attempt int

	if c.conn != nil && !c.conn.IsClosed() {
		return
	}

	logger.Trace("[%s] Connecting to AMQP message broker", c.source)
	for {
		err := c.connect()
		if err == nil {
			logger.Info("[%s] Connected to AMQP message broker", c.source)
			break
		}

		logger.Error("[%s] failed to connect to message broker: %s", c.source, err)
		attempt++
		recovery.BackoffS(attempt)
	}
}

// connect will establish a connection to an AMQP host
func (c *Consumer) connect() (err error) {
	defer func() {
		if reco := recover(); reco != nil {
			err = fmt.Errorf("[%s] panicked connecting to message broker: %v", c.source, reco)
		}
	}()

	logger.Trace("[%s] Dialing... %s", c.source, c.url)
	c.conn, err = amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("[%s] failed to connect to message broker: %s", c.source, err)
	}

	logger.Trace("[%s] Creating channel", c.source)
	c.channel, err = c.conn.Channel()
	if err != nil {
		c.conn.Close()
		return fmt.Errorf("[%s] failed to open a channel: %s", c.source, err)
	}

	logger.Trace("[%s] Setting up notifications", c.source)
	c.connectionCloseNotification = make(chan *amqp.Error)
	c.conn.NotifyClose(c.connectionCloseNotification)

	c.channelCloseNotification = make(chan *amqp.Error)
	c.channel.NotifyClose(c.channelCloseNotification)

	return c.createComponents()
}

// createComponents will do some basic housekeeping before allowing the connection to be used.
func (c *Consumer) createComponents() (err error) {
	err = c.ensureExchangeExists()
	if err != nil {
		c.conn.Close()
		return err
	}

	err = c.ensureQueueBound()
	if err != nil {
		c.conn.Close()
		return err
	}

	err = c.consumeMessages()
	if err != nil {
		c.conn.Close()
		return err
	}

	return err
}

func (c *Consumer) ensureExchangeExists() error {
	logger.Info("[%s] Ensuring that exchange '%s' of type '%s' exists", c.source, c.config.exchangeName, c.config.exchangeType)
	err := ensureExchangeExists(c.channel, c.config.exchangeName, c.config.exchangeType)

	if err != nil {
		return fmt.Errorf("[%s] failed to declare '%s' exchange '%s': %s", c.source, c.config.exchangeType, c.config.exchangeName, err)
	}

	return nil
}

func (c *Consumer) ensureQueueBound() error {
	q, err := c.channel.QueueDeclare(
		c.config.queueName, // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return fmt.Errorf("[%s] failed to declare queue '%s': %s", c.source, c.config.queueName, err)
	}
	c.queue = &q

	err = c.channel.QueueBind(
		q.Name,
		c.config.queueBindingKey,
		c.config.exchangeName,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("[%s] failed to bind queue '%s' to exchange '%s' with key '%s': %s", c.source, q.Name, c.config.exchangeName, c.config.queueBindingKey, err)
	}

	return nil
}

func (c *Consumer) consumeMessages() error {
	msgs, err := c.channel.Consume(
		c.config.queueName,       // queue
		c.config.messageConsumer, // consumer
		false,                    // auto-ack
		false,                    // exclusive
		false,                    // no-local
		false,                    // no-wait
		nil,                      // args
	)

	if err != nil {
		return fmt.Errorf("[%s] failed to register a consumer on queue: '%s': %s", c.source, c.config.queueName, err)
	}

	c.messages = msgs

	return nil
}

func (c *Consumer) writeHeartbeat() {
	c.LastHeartbeat = clock.Now()
}
