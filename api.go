// Package ergo provides a declarative event mesh over capitan and herald.
//
// ergo eliminates distributed event wiring boilerplate by making event flow
// declarative. The application author defines what signals go out and what
// streams come in — ergo handles the transport invisibly.
//
//	mesh := ergo.New(factory)
//	ergo.Out[Order](mesh, "orders", OrderCreated, OrderKey)
//	ergo.In[Payment](mesh, "payments", PaymentReceived, PaymentKey)
//	mesh.Start(ctx)
//	defer mesh.Close()
//
// Out declares that when a capitan signal fires locally, ergo publishes it
// to an external stream. Application code continues to use capitan.Emit as normal.
//
// In declares that an external stream should be consumed and emitted as a
// local capitan signal. Handler code uses standard capitan.Hook.
package ergo

import (
	"context"

	"github.com/zoobz-io/herald"
)

// ProviderFactory creates a herald Provider for a given stream name.
// The factory is called once per channel during Mesh.Start.
type ProviderFactory func(stream string) herald.Provider

// direction indicates whether a channel publishes or subscribes.
type direction int

const (
	dirOut direction = iota
	dirIn
)

// channel is the non-generic interface that allows Mesh to hold heterogeneous channels.
type channel interface {
	streamName() string
	dir() direction
	signalName() string
	validate() error
	start(ctx context.Context) error
	close() error
}
