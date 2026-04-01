package ergo

import "github.com/zoobz-io/capitan"

// ErrorSignal is emitted when ergo encounters an operational error.
// Hook into this signal to observe publish failures, subscribe errors, and unmarshal failures.
var (
	ErrorSignal = capitan.NewSignal("ergo.error", "Ergo operational error")
	ErrorKey    = capitan.NewKey[Error]("error", "ergo.Error")
)

// Error represents an operational error in ergo.
type Error struct {
	// Operation is the operation that failed: "publish", "subscribe", "unmarshal", "ack", "nack".
	Operation string `json:"operation"`

	// Stream is the stream name involved in the error.
	Stream string `json:"stream"`

	// Signal is the name of the capitan signal involved.
	Signal string `json:"signal"`

	// Err is the error message.
	Err string `json:"error"`

	// Raw contains the original message bytes, if available.
	Raw []byte `json:"raw,omitempty"`
}
