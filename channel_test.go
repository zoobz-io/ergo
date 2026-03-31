//go:build testing

package ergo

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/herald"
	helpers "github.com/zoobz-io/ergo/testing"
)

type testPayload struct {
	ID    string `json:"id"`
	Value int    `json:"value"`
}

func TestOut_EmitPublishes(t *testing.T) {
	c := testCapitan(t)
	f := helpers.NewMockProviderFactory()
	p := helpers.NewMockProvider()
	f.Register("orders", p)

	m := New(f.Factory(), WithCapitan(c))

	signal := capitan.NewSignal("order.created", "Order created")
	key := capitan.NewKey[testPayload]("order", "ergo.testPayload")

	helpers.AssertNoError(t, Out(m, "orders", signal, key,
		WithPipeline[testPayload](), // No retry — immediate, for test determinism.
	))
	helpers.AssertNoError(t, m.Start(context.Background()))
	defer m.Close()

	// Emit a signal — should be published to the provider.
	c.Emit(context.Background(), signal, key.Field(testPayload{ID: "ord-1", Value: 42}))

	// In sync mode, the observer callback runs immediately.
	published := p.Published()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}

	var got testPayload
	if err := json.Unmarshal(published[0].Data, &got); err != nil {
		t.Fatalf("unmarshal published data: %v", err)
	}
	helpers.AssertEqual(t, got.ID, "ord-1")
	helpers.AssertEqual(t, got.Value, 42)
	helpers.AssertEqual(t, published[0].Metadata["Content-Type"], "application/json")
}

func TestOut_PublishError_EmitsErrorSignal(t *testing.T) {
	c := testCapitan(t)
	f := helpers.NewMockProviderFactory()
	p := helpers.NewMockProvider().WithPublishError(errForTest)
	f.Register("orders", p)

	m := New(f.Factory(), WithCapitan(c))

	signal := capitan.NewSignal("order.created", "Order created")
	key := capitan.NewKey[testPayload]("order", "ergo.testPayload")

	// Capture error signals with channel for synchronization.
	errCh := make(chan Error, 1)
	c.Hook(ErrorSignal, func(_ context.Context, e *capitan.Event) {
		v, ok := ErrorKey.From(e)
		if ok {
			errCh <- v
		}
	})

	helpers.AssertNoError(t, Out(m, "orders", signal, key,
		WithPipeline[testPayload](), // No retry.
	))
	helpers.AssertNoError(t, m.Start(context.Background()))
	defer m.Close()

	c.Emit(context.Background(), signal, key.Field(testPayload{ID: "ord-1", Value: 1}))

	// In sync mode the error signal is emitted synchronously within the observer callback.
	select {
	case capturedErr := <-errCh:
		helpers.AssertEqual(t, capturedErr.Operation, "publish")
		helpers.AssertEqual(t, capturedErr.Stream, "orders")
		helpers.AssertEqual(t, capturedErr.Signal, "order.created")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for error signal")
	}
}

var errForTest = errorString("test publish error")

type errorString string

func (e errorString) Error() string { return string(e) }

func TestIn_MessageEmitsSignal(t *testing.T) {
	c := testCapitan(t)
	f := helpers.NewMockProviderFactory()
	p := helpers.NewMockProvider()
	f.Register("payments", p)

	m := New(f.Factory(), WithCapitan(c))

	signal := capitan.NewSignal("payment.received", "Payment received")
	key := capitan.NewKey[testPayload]("payment", "ergo.testPayload")

	// Capture emitted events.
	var received testPayload
	var receivedMeta herald.Metadata
	c.Hook(signal, func(_ context.Context, e *capitan.Event) {
		v, ok := key.From(e)
		if ok {
			received = v
		}
		meta, ok := herald.MetadataKey.From(e)
		if ok {
			receivedMeta = meta
		}
	})

	helpers.AssertNoError(t, In(m, "payments", signal, key,
		WithPipeline[testPayload](), // No retry.
	))
	helpers.AssertNoError(t, m.Start(context.Background()))
	defer m.Close()

	// Send a message through the provider.
	data, _ := json.Marshal(testPayload{ID: "pay-1", Value: 100})
	acked, _ := p.SendMessageWithAck(data, herald.Metadata{"trace-id": "abc"})

	// Wait for ack.
	select {
	case <-acked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ack")
	}

	helpers.AssertEqual(t, received.ID, "pay-1")
	helpers.AssertEqual(t, received.Value, 100)
	helpers.AssertEqual(t, receivedMeta["trace-id"], "abc")
}

func TestIn_UnmarshalError_Nacks(t *testing.T) {
	c := testCapitan(t)
	f := helpers.NewMockProviderFactory()
	p := helpers.NewMockProvider()
	f.Register("payments", p)

	m := New(f.Factory(), WithCapitan(c))

	signal := capitan.NewSignal("payment.received", "Payment received")
	key := capitan.NewKey[testPayload]("payment", "ergo.testPayload")

	// Capture error signals with proper synchronization.
	errCh := make(chan Error, 1)
	c.Hook(ErrorSignal, func(_ context.Context, e *capitan.Event) {
		v, ok := ErrorKey.From(e)
		if ok {
			errCh <- v
		}
	})

	helpers.AssertNoError(t, In(m, "payments", signal, key,
		WithPipeline[testPayload](), // No retry.
	))
	helpers.AssertNoError(t, m.Start(context.Background()))
	defer m.Close()

	// Send invalid data.
	p.SendMessage([]byte("not json"), nil)

	// Wait for the error signal.
	select {
	case capturedErr := <-errCh:
		helpers.AssertEqual(t, capturedErr.Operation, "unmarshal")
		helpers.AssertEqual(t, capturedErr.Stream, "payments")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for error signal")
	}
}

func TestIn_ContextCancel_StopsSubscriber(t *testing.T) {
	c := testCapitan(t)
	f := helpers.NewMockProviderFactory()
	p := helpers.NewMockProvider()
	f.Register("events", p)

	m := New(f.Factory(), WithCapitan(c))

	signal := capitan.NewSignal("event.received", "Event received")
	key := capitan.NewKey[testPayload]("event", "ergo.testPayload")

	helpers.AssertNoError(t, In(m, "events", signal, key,
		WithPipeline[testPayload](),
	))

	ctx, cancel := context.WithCancel(context.Background())
	helpers.AssertNoError(t, m.Start(ctx))

	// Cancel the context — subscriber should stop.
	cancel()

	// Close should complete without hanging.
	helpers.AssertNoError(t, m.Close())
}

func TestMesh_CloseOrder_OutBeforeIn(t *testing.T) {
	c := testCapitan(t)
	f := helpers.NewMockProviderFactory()
	outP := helpers.NewMockProvider()
	inP := helpers.NewMockProvider()
	f.Register("out-stream", outP)
	f.Register("in-stream", inP)

	m := New(f.Factory(), WithCapitan(c))

	outSignal := capitan.NewSignal("out.signal", "Outbound")
	outKey := capitan.NewKey[testPayload]("out", "ergo.testPayload")
	inSignal := capitan.NewSignal("in.signal", "Inbound")
	inKey := capitan.NewKey[testPayload]("in", "ergo.testPayload")

	helpers.AssertNoError(t, Out(m, "out-stream", outSignal, outKey, WithPipeline[testPayload]()))
	helpers.AssertNoError(t, In(m, "in-stream", inSignal, inKey, WithPipeline[testPayload]()))
	helpers.AssertNoError(t, m.Start(context.Background()))

	// Close should not hang — out observers close before in subscribers.
	helpers.AssertNoError(t, m.Close())
}
