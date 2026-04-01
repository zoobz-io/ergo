//go:build testing

package testing

import (
	"context"
	"errors"
	"testing"

	"github.com/zoobz-io/herald"
)

func TestAssertNoError(t *testing.T) {
	AssertNoError(t, nil)
}

func TestAssertError(t *testing.T) {
	AssertError(t, errors.New("test error"))
}

func TestAssertEqual(t *testing.T) {
	AssertEqual(t, 1, 1)
	AssertEqual(t, "foo", "foo")
	AssertEqual(t, true, true)
}

func TestMockProvider_Publish(t *testing.T) {
	p := NewMockProvider()
	err := p.Publish(context.Background(), []byte("hello"), herald.Metadata{"key": "val"})
	AssertNoError(t, err)

	published := p.Published()
	AssertEqual(t, len(published), 1)
	AssertEqual(t, string(published[0].Data), "hello")
	AssertEqual(t, published[0].Metadata["key"], "val")
}

func TestMockProvider_PublishError(t *testing.T) {
	p := NewMockProvider().WithPublishError(errors.New("fail"))
	err := p.Publish(context.Background(), []byte("hello"), nil)
	AssertError(t, err)
	AssertEqual(t, len(p.Published()), 0)
}

func TestMockProvider_Ping(t *testing.T) {
	p := NewMockProvider()
	AssertNoError(t, p.Ping(context.Background()))

	p2 := NewMockProvider().WithPingError(errors.New("down"))
	AssertError(t, p2.Ping(context.Background()))
}

func TestMockProviderFactory(t *testing.T) {
	f := NewMockProviderFactory()
	p := NewMockProvider()
	f.Register("orders", p)

	factory := f.Factory()

	// Registered stream returns the registered provider.
	got := factory("orders")
	AssertEqual(t, got == p, true)

	// Unknown stream creates a new mock.
	got2 := factory("payments")
	AssertEqual(t, got2 != nil, true)
	AssertEqual(t, f.Provider("payments") != nil, true)
}
