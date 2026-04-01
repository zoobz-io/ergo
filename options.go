package ergo

import (
	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/herald"
)

// Option configures a Mesh.
type Option func(*Mesh)

// WithCapitan sets a custom Capitan instance for the mesh.
// If not specified, capitan.Default() is used.
func WithCapitan(c *capitan.Capitan) Option {
	return func(m *Mesh) {
		m.capitan = c
	}
}

// WithCodec sets the default codec for all channels.
// If not specified, herald.JSONCodec{} is used.
// Per-channel codecs override this default.
func WithCodec(c herald.Codec) Option {
	return func(m *Mesh) {
		m.codec = c
	}
}

// ChannelOption configures an individual In or Out channel.
type ChannelOption[T any] func(*channelConfig[T])

// channelConfig holds per-channel configuration.
type channelConfig[T any] struct {
	codec        herald.Codec
	pipelineOpts []herald.Option[T]
}

// WithChannelCodec sets a custom codec for this channel.
// Overrides the mesh-level codec.
func WithChannelCodec[T any](c herald.Codec) ChannelOption[T] {
	return func(cfg *channelConfig[T]) {
		cfg.codec = c
	}
}

// WithPipeline sets reliability options for this channel.
// Overrides the default retry/backoff behavior.
func WithPipeline[T any](opts ...herald.Option[T]) ChannelOption[T] {
	return func(cfg *channelConfig[T]) {
		cfg.pipelineOpts = opts
	}
}
