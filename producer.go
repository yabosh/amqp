package amqp

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
	"github.com/yabosh/go-clock"
	"github.com/yabosh/logger"
	"github.com/yabosh/recovery"
)

// Producer represents a user that produces messages to be published to an AMQP queue.
type Producer struct {
	LastHeartbeat               time.Time
	config                      ProducerConfig
	conn                        *amqp.Connection
	channel                     *amqp.Channel
	connectionCloseNotification chan *amqp.Error
	channelCloseNotification    chan *amqp.Error
	heartbeatTicker             *time.Ticker
	heartbeatExpiration         time.Duration
	url                         string
}

// ProducerConfig is configuration that is specific to outgoing connections to an AMQP host
type ProducerConfig struct {
	Source            string
	ExchangeName      string
	ExchangeType      string
	PublishingChannel chan AMQPPayload
}

// AMQPPayload represents a single message to be published to an AMQP queue
type AMQPPayload struct {
	RoutingKey string
	Body       string
}

// NewProducer creates a new, initialized instance of AMQPPublsher
func NewProducer(broker BrokerConfig, config ProducerConfig) *Producer {
	if config.PublishingChannel == nil {
		panic("AMQP Producer requires a valid input channel")
	}

	return &Producer{
		config: config,
		url:    broker.url(),
	}
}

// Start will connect to AMQP and start a loop to publish any message retrieved from the input channel
func (p *Producer) Start() {
	p.connectWithRetry()
	p.publishLoop()
}

// IsConnected returns true if the publisher is connected to an AMQP instance and false otherwise
func (p *Producer) IsConnected() bool {
	if p == nil || p.conn == nil || p.conn.IsClosed() {
		return false
	}

	return true
}

// publishLoop will read messages from the publishing channel and it will send them to the
// output AMQP queue.
func (p *Producer) publishLoop() {
	defer p.recover("panicked receiving message from broker:")
	p.heartbeatExpiration = hearbeatInterval
	p.heartbeatTicker = time.NewTicker(p.heartbeatExpiration)
	defer p.heartbeatTicker.Stop()

	for {
		select {
		case payload := <-p.config.PublishingChannel:
			p.publish(payload.Body, payload.RoutingKey)
			logger.Debug("[%s] Published message to exchange %s", p.config.Source, p.config.ExchangeName)

		case connErr := <-p.connectionCloseNotification:
			// This is necessary because the amqp library will immediately
			// close this channel after posting the error so this
			// case path will always be taken and cause an infinite loop
			p.connectionCloseNotification = make(chan *amqp.Error)
			p.conn = nil
			p.channel = nil

			if connErr != nil {
				logger.Error("[%s] connection closed %#v", p.config.Source, connErr)
			}

			p.connectWithRetry()

		case reason := <-p.channelCloseNotification:
			logger.Debug("[%s] Channel closed %#v", p.config.Source, reason)
			p.channel = nil
			p.channelCloseNotification = make(chan *amqp.Error)
			if !p.conn.IsClosed() {
				logger.Debug("[%s] Closing connection to message broker", p.config.Source)
				// Closing the channel will force a reconnect which will address some issues such as a missing exchange, etc.
				p.conn.Close()
			}

		case <-p.heartbeatTicker.C:
		}

		p.writeHeartbeat()
	}
}

func (p *Producer) publish(body string, routingKey string) (err error) {
	if err = p.channel.Publish(
		p.config.ExchangeName, // publish to an exchange
		routingKey,            // routing to 0 or more queues
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		return fmt.Errorf("exchange Publish: %s", err)
	}

	return nil
}

func (p *Producer) recover(msg string) {
	if err := recover(); err != nil {
		logger.Error(msg, err)
	}
}

func (p *Producer) connectWithRetry() {
	var attempt int

	if p.conn != nil && !p.conn.IsClosed() {
		return
	}

	logger.Trace("[%s] Connecting to AMQP message broker", p.config.Source)
	for {
		err := p.connect()
		if err == nil {
			logger.Info("[%s] Connected to AMQP message broker", p.config.Source)
			break
		}

		logger.Error("[%s] failed to connect to message broker: %s", p.config.Source, err)
		attempt++
		recovery.BackoffS(attempt)
	}
}

func (p *Producer) connect() (err error) {
	defer func() {
		if reco := recover(); reco != nil {
			err = fmt.Errorf("panicked connecting to message broker: %v", reco)
		}
	}()

	logger.Trace("[%s] Dialing... %s", p.config.Source, p.url)
	p.conn, err = amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("[%s] failed to connect to message broker: %s", p.config.Source, err)
	}

	logger.Trace("[%s] Creating AMQP channel channel", p.config.Source)
	p.channel, err = p.conn.Channel()
	if err != nil {
		p.conn.Close()
		return fmt.Errorf("[%s] failed to open a channel: %s", p.config.Source, err)
	}

	logger.Trace("[%s] Setting up notifications", p.config.Source)
	p.connectionCloseNotification = make(chan *amqp.Error)
	p.conn.NotifyClose(p.connectionCloseNotification)

	p.channelCloseNotification = make(chan *amqp.Error)
	p.channel.NotifyClose(p.channelCloseNotification)

	err = p.ensureExchangeExists()
	if err != nil {
		p.conn.Close()
		return err
	}

	return nil
}

func (p *Producer) ensureExchangeExists() error {
	logger.Info("[%s] Ensuring that exchange '%s' of type '%s' exists", p.config.Source, p.config.ExchangeName, p.config.ExchangeType)
	err := ensureExchangeExists(p.channel, p.config.ExchangeName, p.config.ExchangeType)

	if err != nil {
		return fmt.Errorf("[%s] failed to declare '%s' exchange '%s': %s", p.config.Source, p.config.ExchangeType, p.config.ExchangeName, err)
	}

	return nil
}

func (p *Producer) writeHeartbeat() {
	p.LastHeartbeat = clock.Now()
}
