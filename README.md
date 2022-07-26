# AMQP Interface

This library is a wrapper around the `streadway` AMQP library that provides an interface for a relatively simple set of use cases.

The library is designed to be resilient and self-healing after failures.  It will automatically reconnect to the underlying AMQP host if it is disconnected.

## Consuming

This library assumes that the user wants to consume basic messages from an AMQP exchange.  The following conditions are assumed:

- Messages will be consumed and immediately acknowledged.
- Messages bodies represent JSON
- A single queue, with a single routing-key to an exchange is sufficient
- If the required AMQP exchange does not exist, it will be created

The consumer library uses the Observer pattern to allow messages retrieved from an AMQP queue to be published to the caller.  The caller specifies a function that will be called every time a message is received.  Design note: A go channel could have been used to communicate messages to the caller.  This option was not taken due to the semantics around queue handling, blocking, etc.  By calling a function, the onus is on the caller to handle the dynamics of processing the message or potentially queuing it without the library worrying about blocking.  

The library will block until the message processing function returns.

### Usage

The following example shows how to connect to and use the AMQP library to consume messages.

```go
// Process each incoming message
func ProcessMessage(message *amqp.Delivery) (err error)  {
  fmt.Println(string(message.Body))
}

// Create a new consumer and start a goroutine that will continuously listen for incoming messages
consumer = amqp.NewConsumer(broker, client.ProcessMessage, ExchangeType, AMQPQueueName, BindingKey)
consumer.Start()

// Block forever
<-make(chan bool)

```

## Producing

This library assumes that the user wants to publish basic messages to an AMQP exchange.  The following conditions are assumed:

- Messages will be converted to JSON for publishing
- Messages are transient
- Messages do not have a priority
- Messages support a routing key
- No additional AMQP headers will be added to the outgoing message

### Usage

```go
output := make(chan amqp.AMQPPayload)

 producer := amqp.NewProducer(amqp.ProducerConfig{
  Source:            "myapp",
  ExchangeName:      "myexchange",
  ExchangeType:      "topic",
  PublishingChannel: output,
 })

// Create a goroutine that will listen for messages to be published to AMQP
go producer.Start()

// Send a message
output <- amqp.AMQPPayload{
     RoutingKey: "mykey.mydestination",
     Body:       "sample payload",
    }

```
