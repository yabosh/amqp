package amqp

import "time"

const (
	// heartbeatInterval defines the amount of time between hearbeat messages for the AMQP provider.  Both the
	// consumer and producer will periodically update a timestamp to indicate the the underlying goroutine
	// is still running.  This heartbeat will be updated every time a message is processed or at a minimum
	// it will be updated once per this interval.
	hearbeatInterval = 60 * time.Second
)
