package ergo

import (
	"context"
	"fmt"
	"sync"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/herald"
)

// Mesh manages a collection of event channels that bridge capitan signals
// with external message streams via herald providers.
type Mesh struct {
	factory  ProviderFactory
	capitan  *capitan.Capitan
	codec    herald.Codec
	channels []channel
	streams  map[streamKey]struct{}
	started  bool
	closed   bool
	mu       sync.Mutex
}

// streamKey uniquely identifies a stream registration by name and direction.
type streamKey struct {
	name string
	dir  direction
}

// New creates a Mesh with the given provider factory and options.
func New(factory ProviderFactory, opts ...Option) *Mesh {
	m := &Mesh{
		factory: factory,
		codec:   herald.JSONCodec{},
		streams: make(map[streamKey]struct{}),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// register adds a channel to the mesh. Called by Out and In.
func (m *Mesh) register(ch channel) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrMeshClosed
	}
	if m.started {
		return ErrMeshStarted
	}
	if err := ch.validate(); err != nil {
		return err
	}

	key := streamKey{name: ch.streamName(), dir: ch.dir()}
	if _, exists := m.streams[key]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateStream, ch.streamName())
	}

	m.streams[key] = struct{}{}
	m.channels = append(m.channels, ch)
	return nil
}

// Start validates all channels, creates providers, and activates the mesh.
func (m *Mesh) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrMeshClosed
	}
	if m.started {
		return ErrMeshStarted
	}

	// Start channels, rolling back on failure.
	var started []channel
	for _, ch := range m.channels {
		if err := ch.start(ctx); err != nil {
			// Close already-started channels in reverse order.
			for i := len(started) - 1; i >= 0; i-- {
				_ = started[i].close()
			}
			return fmt.Errorf("ergo: starting channel %q: %w", ch.streamName(), err)
		}
		started = append(started, ch)
	}

	m.started = true
	return nil
}

// Close tears down the mesh, stopping all channels.
// Out channels (observers) are closed first, then In channels (subscribers).
func (m *Mesh) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrMeshClosed
	}
	m.closed = true

	var firstErr error

	// Close Out channels first — stop producing before stopping consumers.
	for _, ch := range m.channels {
		if ch.dir() == dirOut {
			if err := ch.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	// Close In channels.
	for _, ch := range m.channels {
		if ch.dir() == dirIn {
			if err := ch.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// resolveCapitan returns the configured capitan instance or the default.
func (m *Mesh) resolveCapitan() *capitan.Capitan {
	if m.capitan != nil {
		return m.capitan
	}
	return capitan.Default()
}
