package stopcontext_test

import (
	"context"
	"testing"

	"github.com/canastic/pgqueue/stopcontext"
)

func TestBasic(t *testing.T) {
	base := context.Background()
	ctx, stop := stopcontext.WithStop(base)

	select {
	case <-ctx.Stopped():
		t.Error("unexpected Stopped")
	default:
	}

	stop()

	select {
	case <-ctx.Done():
		t.Error("unexpected Done")
	default:
	}

	select {
	case <-ctx.Stopped():
	default:
		t.Error("expected Stopped")
	}
}

func TestParentDone(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	ctx, stop := stopcontext.WithStop(base)
	defer stop()

	cancel()

	select {
	case <-ctx.Done():
	default:
		t.Error("expected Done")
	}

	select {
	case <-ctx.Stopped():
	default:
		t.Error("expected Stopped")
	}
}

func TestChained(t *testing.T) {
	base, stop := stopcontext.WithStop(context.Background())
	ctx, stopChild := stopcontext.WithStop(base)
	defer stopChild()

	stop()

	select {
	case <-ctx.Done():
		t.Error("unexpected Done")
	default:
	}

	select {
	case <-ctx.Stopped():
	default:
	}
}

func TestDoneUnstop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	select {
	case <-stopcontext.Stopped(ctx):
		t.Error("unexpected Done")
	default:
	}

	cancel()

	select {
	case <-stopcontext.Stopped(ctx):
	default:
		t.Error("expected stopcontext.Stopped")
	}
}

func TestDoneGraceful(t *testing.T) {
	ctx, stop := stopcontext.WithStop(context.Background())

	select {
	case <-stopcontext.Stopped(ctx):
		t.Error("unexpected Done")
	default:
	}

	stop()

	select {
	case <-stopcontext.Stopped(ctx):
	default:
		t.Error("expected stopcontext.Stopped")
	}
}

func TestWithValueStoppable(t *testing.T) {
	base, stop := stopcontext.WithStop(context.Background())
	ctx := context.WithValue(base, "foo", "bar")

	if expected, got := "bar", ctx.Value("foo"); expected != got {
		t.Errorf("expected WithValue to set value in new context, got %q", got)
	}

	select {
	case <-stopcontext.Stopped(ctx):
		t.Error("unexpected Done")
	default:
	}

	stop()

	select {
	case <-stopcontext.Stopped(ctx):
	default:
		t.Error("expected stopcontext.Stopped")
	}
}

func TestWithValueNoStoppable(t *testing.T) {
	ctx := context.WithValue(context.Background(), "foo", "bar")

	if expected, got := "bar", ctx.Value("foo"); expected != got {
		t.Errorf("expected WithValue to set value in new context, got %q", got)
	}
}
