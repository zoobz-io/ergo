package ergo

import (
	"context"
	"sync"
	"time"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/herald"
	"github.com/zoobz-io/pipz"
)

// Pipeline identities.
var (
	publishID = pipz.NewIdentity("ergo:publish", "Publishes to broker")
	outPipeID = pipz.NewIdentity("ergo:out", "Out channel pipeline")
	emitID    = pipz.NewIdentity("ergo:emit", "Emits to capitan signal")
	inPipeID  = pipz.NewIdentity("ergo:in", "In channel pipeline")
)

// Default reliability: 3 retries with 500ms exponential backoff.
const (
	defaultMaxAttempts = 3
	defaultBaseDelay   = 500 * time.Millisecond
)

// Out registers an outbound channel: when the given capitan signal fires,
// ergo publishes the value to the named stream.
func Out[T any](m *Mesh, stream string, signal capitan.Signal, key capitan.GenericKey[T], opts ...ChannelOption[T]) error {
	cfg := channelConfig[T]{}
	for _, opt := range opts {
		opt(&cfg)
	}

	ch := &outChannel[T]{
		stream: stream,
		signal: signal,
		key:    key,
		mesh:   m,
		config: cfg,
	}
	return m.register(ch)
}

// In registers an inbound channel: ergo consumes from the named stream
// and emits the value as the given capitan signal.
func In[T any](m *Mesh, stream string, signal capitan.Signal, key capitan.GenericKey[T], opts ...ChannelOption[T]) error {
	cfg := channelConfig[T]{}
	for _, opt := range opts {
		opt(&cfg)
	}

	ch := &inChannel[T]{
		stream: stream,
		signal: signal,
		key:    key,
		mesh:   m,
		config: cfg,
	}
	return m.register(ch)
}

// outChannel publishes capitan signals to an external stream.
type outChannel[T any] struct {
	provider herald.Provider
	codec    herald.Codec
	mesh     *Mesh
	pipeline *pipz.Pipeline[*herald.Envelope[T]]
	observer *capitan.Observer
	signal   capitan.Signal
	key      capitan.GenericKey[T]
	stream   string
	config   channelConfig[T]
	inflight sync.WaitGroup
}

func (o *outChannel[T]) streamName() string { return o.stream }
func (o *outChannel[T]) dir() direction     { return dirOut }
func (o *outChannel[T]) signalName() string { return o.signal.Name() }

func (o *outChannel[T]) validate() error {
	if o.stream == "" {
		return ErrEmptyStream
	}
	return nil
}

func (o *outChannel[T]) start(ctx context.Context) error {
	// Create provider.
	o.provider = o.mesh.factory(o.stream)

	// Ping to verify connectivity.
	if err := o.provider.Ping(ctx); err != nil {
		return err
	}

	// Resolve codec: channel override > mesh default.
	o.codec = o.mesh.codec
	if o.config.codec != nil {
		o.codec = o.config.codec
	}

	// Build pipeline: terminal + options.
	terminal := newPublishTerminal[T](o.provider, o.codec)

	pipelineOpts := o.config.pipelineOpts
	if len(pipelineOpts) == 0 {
		// Default reliability: 3 retries, 500ms exponential backoff.
		pipelineOpts = []herald.Option[T]{
			herald.WithBackoff[T](defaultMaxAttempts, defaultBaseDelay),
		}
	}
	chain := terminal
	for _, opt := range pipelineOpts {
		chain = opt(chain)
	}
	o.pipeline = pipz.NewPipeline(outPipeID, chain)

	// Register observer for this signal only.
	capi := o.mesh.resolveCapitan()
	o.observer = capi.Observe(func(_ context.Context, e *capitan.Event) {
		value, ok := o.key.From(e)
		if !ok {
			return
		}

		o.inflight.Add(1)
		defer o.inflight.Done()

		env := &herald.Envelope[T]{
			Value:    value,
			Metadata: make(herald.Metadata),
		}

		_, err := o.pipeline.Process(e.Context(), env)
		if err != nil {
			o.emitError(e.Context(), "publish", err.Error(), nil)
		}
	}, o.signal)

	return nil
}

func (o *outChannel[T]) close() error {
	if o.observer != nil {
		o.observer.Close()
	}
	o.inflight.Wait()
	if o.pipeline != nil {
		return o.pipeline.Close()
	}
	return nil
}

func (o *outChannel[T]) emitError(ctx context.Context, operation, errMsg string, raw []byte) {
	capi := o.mesh.resolveCapitan()
	capi.Emit(ctx, ErrorSignal, ErrorKey.Field(Error{
		Operation: operation,
		Stream:    o.stream,
		Signal:    o.signal.Name(),
		Err:       errMsg,
		Raw:       raw,
	}))
}

// inChannel consumes from an external stream and emits to capitan.
type inChannel[T any] struct {
	provider herald.Provider
	codec    herald.Codec
	mesh     *Mesh
	pipeline *pipz.Pipeline[*herald.Envelope[T]]
	cancel   context.CancelFunc
	signal   capitan.Signal
	key      capitan.GenericKey[T]
	stream   string
	config   channelConfig[T]
	wg       sync.WaitGroup
}

func (i *inChannel[T]) streamName() string { return i.stream }
func (i *inChannel[T]) dir() direction     { return dirIn }
func (i *inChannel[T]) signalName() string { return i.signal.Name() }

func (i *inChannel[T]) validate() error {
	if i.stream == "" {
		return ErrEmptyStream
	}
	return nil
}

func (i *inChannel[T]) start(ctx context.Context) error {
	// Create provider.
	i.provider = i.mesh.factory(i.stream)

	// Ping to verify connectivity.
	if err := i.provider.Ping(ctx); err != nil {
		return err
	}

	// Resolve codec.
	i.codec = i.mesh.codec
	if i.config.codec != nil {
		i.codec = i.config.codec
	}

	// Build pipeline: terminal emits to capitan.
	capi := i.mesh.resolveCapitan()
	terminal := newEmitTerminal(capi, i.signal, i.key)

	pipelineOpts := i.config.pipelineOpts
	if len(pipelineOpts) == 0 {
		pipelineOpts = []herald.Option[T]{
			herald.WithBackoff[T](defaultMaxAttempts, defaultBaseDelay),
		}
	}
	chain := terminal
	for _, opt := range pipelineOpts {
		chain = opt(chain)
	}
	i.pipeline = pipz.NewPipeline(inPipeID, chain)

	// Start subscriber goroutine.
	subCtx, cancel := context.WithCancel(ctx)
	i.cancel = cancel

	messages := i.provider.Subscribe(subCtx)

	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		for {
			select {
			case <-subCtx.Done():
				return
			case result, ok := <-messages:
				if !ok {
					return
				}
				if result.IsError() {
					i.emitError(subCtx, "subscribe", result.Error().Error(), nil)
					continue
				}
				i.process(subCtx, result.Value())
			}
		}
	}()

	return nil
}

func (i *inChannel[T]) process(ctx context.Context, msg herald.Message) {
	var value T
	if err := i.codec.Unmarshal(msg.Data, &value); err != nil {
		if msg.Nack != nil {
			_ = msg.Nack()
		}
		i.emitError(ctx, "unmarshal", err.Error(), msg.Data)
		return
	}

	env := &herald.Envelope[T]{
		Value:    value,
		Metadata: copyMetadata(msg.Metadata),
	}

	_, err := i.pipeline.Process(ctx, env)
	if err != nil {
		if msg.Nack != nil {
			_ = msg.Nack()
		}
		i.emitError(ctx, "subscribe", err.Error(), nil)
	} else if msg.Ack != nil {
		if ackErr := msg.Ack(); ackErr != nil {
			i.emitError(ctx, "ack", ackErr.Error(), nil)
		}
	}
}

func (i *inChannel[T]) close() error {
	if i.cancel != nil {
		i.cancel()
	}
	i.wg.Wait()
	if i.pipeline != nil {
		return i.pipeline.Close()
	}
	return nil
}

func (i *inChannel[T]) emitError(ctx context.Context, operation, errMsg string, raw []byte) {
	capi := i.mesh.resolveCapitan()
	capi.Emit(ctx, ErrorSignal, ErrorKey.Field(Error{
		Operation: operation,
		Stream:    i.stream,
		Signal:    i.signal.Name(),
		Err:       errMsg,
		Raw:       raw,
	}))
}

// newPublishTerminal creates the terminal pipz processor that serializes and publishes.
func newPublishTerminal[T any](provider herald.Provider, codec herald.Codec) pipz.Chainable[*herald.Envelope[T]] {
	return pipz.Apply(publishID, func(ctx context.Context, env *herald.Envelope[T]) (*herald.Envelope[T], error) {
		data, err := codec.Marshal(env.Value)
		if err != nil {
			return env, err
		}
		metadata := copyMetadata(env.Metadata)
		if _, exists := metadata["Content-Type"]; !exists {
			metadata["Content-Type"] = codec.ContentType()
		}
		err = provider.Publish(ctx, data, metadata)
		return env, err
	})
}

// newEmitTerminal creates the terminal pipz processor that emits to capitan.
func newEmitTerminal[T any](capi *capitan.Capitan, signal capitan.Signal, key capitan.GenericKey[T]) pipz.Chainable[*herald.Envelope[T]] {
	return pipz.Effect(emitID, func(ctx context.Context, env *herald.Envelope[T]) error {
		capi.Emit(ctx, signal, key.Field(env.Value), herald.MetadataKey.Field(env.Metadata))
		return nil
	})
}

// copyMetadata returns a shallow copy of the metadata, or a new map if nil.
func copyMetadata(m herald.Metadata) herald.Metadata {
	if m == nil {
		return make(herald.Metadata)
	}
	copied := make(herald.Metadata, len(m))
	for k, v := range m {
		copied[k] = v
	}
	return copied
}
