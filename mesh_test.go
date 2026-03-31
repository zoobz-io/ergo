//go:build testing

package ergo

import (
	"context"
	"errors"
	"testing"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/herald"
	helpers "github.com/zoobz-io/ergo/testing"
)

func testCapitan(t *testing.T) *capitan.Capitan {
	t.Helper()
	c := capitan.New(capitan.WithSyncMode())
	t.Cleanup(c.Shutdown)
	return c
}

func testMesh(t *testing.T, c *capitan.Capitan) (*Mesh, *helpers.MockProviderFactory) {
	t.Helper()
	f := helpers.NewMockProviderFactory()
	m := New(f.Factory(), WithCapitan(c))
	return m, f
}

func TestNew_Defaults(t *testing.T) {
	f := helpers.NewMockProviderFactory()
	m := New(f.Factory())

	if m.factory == nil {
		t.Fatal("factory should not be nil")
	}
	if m.codec == nil {
		t.Fatal("codec should not be nil")
	}
}

func TestNew_WithOptions(t *testing.T) {
	c := testCapitan(t)
	f := helpers.NewMockProviderFactory()
	codec := herald.JSONCodec{}
	m := New(f.Factory(), WithCapitan(c), WithCodec(codec))

	if m.capitan != c {
		t.Fatal("capitan should be the provided instance")
	}
}

func TestMesh_StartEmpty(t *testing.T) {
	c := testCapitan(t)
	m, _ := testMesh(t, c)

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start empty mesh: %v", err)
	}
}

func TestMesh_StartTwice(t *testing.T) {
	c := testCapitan(t)
	m, _ := testMesh(t, c)

	helpers.AssertNoError(t, m.Start(context.Background()))

	err := m.Start(context.Background())
	if !errors.Is(err, ErrMeshStarted) {
		t.Fatalf("expected ErrMeshStarted, got %v", err)
	}
}

func TestMesh_CloseTwice(t *testing.T) {
	c := testCapitan(t)
	m, _ := testMesh(t, c)

	helpers.AssertNoError(t, m.Start(context.Background()))
	helpers.AssertNoError(t, m.Close())

	err := m.Close()
	if !errors.Is(err, ErrMeshClosed) {
		t.Fatalf("expected ErrMeshClosed, got %v", err)
	}
}

func TestMesh_RegisterAfterStart(t *testing.T) {
	c := testCapitan(t)
	m, _ := testMesh(t, c)

	helpers.AssertNoError(t, m.Start(context.Background()))

	signal := capitan.NewSignal("test.signal", "test")
	key := capitan.NewKey[string]("value", "string")
	err := Out(m, "stream", signal, key)
	if !errors.Is(err, ErrMeshStarted) {
		t.Fatalf("expected ErrMeshStarted, got %v", err)
	}
}

func TestMesh_RegisterAfterClose(t *testing.T) {
	c := testCapitan(t)
	m, _ := testMesh(t, c)

	helpers.AssertNoError(t, m.Start(context.Background()))
	helpers.AssertNoError(t, m.Close())

	signal := capitan.NewSignal("test.signal", "test")
	key := capitan.NewKey[string]("value", "string")
	err := Out(m, "stream", signal, key)
	if !errors.Is(err, ErrMeshClosed) {
		t.Fatalf("expected ErrMeshClosed, got %v", err)
	}
}

func TestMesh_DuplicateStream(t *testing.T) {
	c := testCapitan(t)
	m, _ := testMesh(t, c)

	signal := capitan.NewSignal("test.signal", "test")
	key := capitan.NewKey[string]("value", "string")

	helpers.AssertNoError(t, Out(m, "orders", signal, key))

	err := Out(m, "orders", signal, key)
	if !errors.Is(err, ErrDuplicateStream) {
		t.Fatalf("expected ErrDuplicateStream, got %v", err)
	}
}

func TestMesh_SameStreamDifferentDirection(t *testing.T) {
	c := testCapitan(t)
	m, _ := testMesh(t, c)

	outSignal := capitan.NewSignal("out.signal", "outbound")
	outKey := capitan.NewKey[string]("value", "string")
	inSignal := capitan.NewSignal("in.signal", "inbound")
	inKey := capitan.NewKey[string]("value", "string")

	helpers.AssertNoError(t, Out(m, "events", outSignal, outKey))
	helpers.AssertNoError(t, In(m, "events", inSignal, inKey))
}

func TestMesh_EmptyStream(t *testing.T) {
	c := testCapitan(t)
	m, _ := testMesh(t, c)

	signal := capitan.NewSignal("test.signal", "test")
	key := capitan.NewKey[string]("value", "string")

	err := Out(m, "", signal, key)
	if !errors.Is(err, ErrEmptyStream) {
		t.Fatalf("expected ErrEmptyStream, got %v", err)
	}
}

func TestMesh_StartPingFailure(t *testing.T) {
	c := testCapitan(t)
	f := helpers.NewMockProviderFactory()
	p := helpers.NewMockProvider().WithPingError(errors.New("connection refused"))
	f.Register("orders", p)

	m := New(f.Factory(), WithCapitan(c))

	signal := capitan.NewSignal("order.created", "test")
	key := capitan.NewKey[string]("order", "string")
	helpers.AssertNoError(t, Out(m, "orders", signal, key))

	err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected error from ping failure")
	}
}
