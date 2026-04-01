//go:build testing

// Package testing provides test helpers for ergo.
package testing

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/herald"
)

// MockProvider implements herald.Provider for testing.
type MockProvider struct {
	published []PublishedMessage
	messages  chan herald.Result[herald.Message]
	pingErr   error
	pubErr    error
	mu        sync.Mutex
}

// PublishedMessage records a message sent via Publish.
type PublishedMessage struct {
	Data     []byte
	Metadata herald.Metadata
}

// NewMockProvider creates a MockProvider with a buffered message channel.
func NewMockProvider() *MockProvider {
	return &MockProvider{
		messages: make(chan herald.Result[herald.Message], 100),
	}
}

// WithPingError configures the provider to return an error on Ping.
func (m *MockProvider) WithPingError(err error) *MockProvider {
	m.pingErr = err
	return m
}

// WithPublishError configures the provider to return an error on Publish.
func (m *MockProvider) WithPublishError(err error) *MockProvider {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pubErr = err
	return m
}

func (m *MockProvider) Publish(_ context.Context, data []byte, metadata herald.Metadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pubErr != nil {
		return m.pubErr
	}
	m.published = append(m.published, PublishedMessage{Data: data, Metadata: metadata})
	return nil
}

func (m *MockProvider) Subscribe(_ context.Context) <-chan herald.Result[herald.Message] {
	return m.messages
}

func (m *MockProvider) Ping(_ context.Context) error {
	return m.pingErr
}

func (m *MockProvider) Close() error {
	return nil
}

// Published returns all messages published to this provider.
func (m *MockProvider) Published() []PublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]PublishedMessage, len(m.published))
	copy(out, m.published)
	return out
}

// SendMessage sends a message into the subscription channel.
func (m *MockProvider) SendMessage(data []byte, metadata herald.Metadata) {
	m.messages <- herald.NewSuccess(herald.Message{
		Data:     data,
		Metadata: metadata,
		Ack:      func() error { return nil },
		Nack:     func() error { return nil },
	})
}

// SendMessageWithAck sends a message and returns channels to observe ack/nack.
func (m *MockProvider) SendMessageWithAck(data []byte, metadata herald.Metadata) (acked, nacked chan struct{}) {
	acked = make(chan struct{}, 1)
	nacked = make(chan struct{}, 1)
	m.messages <- herald.NewSuccess(herald.Message{
		Data:     data,
		Metadata: metadata,
		Ack:      func() error { acked <- struct{}{}; return nil },
		Nack:     func() error { nacked <- struct{}{}; return nil },
	})
	return acked, nacked
}

// SendError sends an error into the subscription channel.
func (m *MockProvider) SendError(err error) {
	m.messages <- herald.NewError[herald.Message](err)
}

// MockProviderFactory creates a factory that returns pre-configured MockProviders by stream name.
type MockProviderFactory struct {
	providers map[string]*MockProvider
	mu        sync.Mutex
}

// NewMockProviderFactory creates a new factory.
func NewMockProviderFactory() *MockProviderFactory {
	return &MockProviderFactory{
		providers: make(map[string]*MockProvider),
	}
}

// Register associates a MockProvider with a stream name.
func (f *MockProviderFactory) Register(stream string, provider *MockProvider) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.providers[stream] = provider
}

// Factory returns the ProviderFactory function for use with ergo.New.
func (f *MockProviderFactory) Factory() func(string) herald.Provider {
	return func(stream string) herald.Provider {
		f.mu.Lock()
		defer f.mu.Unlock()
		if p, ok := f.providers[stream]; ok {
			return p
		}
		p := NewMockProvider()
		f.providers[stream] = p
		return p
	}
}

// Provider returns the MockProvider for a stream, or nil.
func (f *MockProviderFactory) Provider(stream string) *MockProvider {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.providers[stream]
}

// SignalCapture captures events for a specific signal on a capitan instance.
type SignalCapture struct {
	events [][]capitan.Field
	mu     sync.Mutex
}

// NewSignalCapture hooks into the given signal on the capitan instance and captures events.
func NewSignalCapture(c *capitan.Capitan, signal capitan.Signal) *SignalCapture {
	sc := &SignalCapture{}
	c.Hook(signal, func(_ context.Context, e *capitan.Event) {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		sc.events = append(sc.events, e.Fields())
	})
	return sc
}

// Count returns the number of captured events.
func (sc *SignalCapture) Count() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return len(sc.events)
}

// WaitForCount blocks until the capture has at least n events or timeout.
func (sc *SignalCapture) WaitForCount(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if sc.Count() >= n {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d events, got %d", n, sc.Count())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// AssertNoError fails the test if err is not nil.
func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// AssertError fails the test if err is nil.
func AssertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// AssertEqual fails the test if got != want.
func AssertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
