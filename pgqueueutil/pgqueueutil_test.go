package pgqueueutil

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/canastic/pgqueue"
)

func TestRequeueOnError(t *testing.T) {
	type ctxKey struct{}
	ctxValue := "ctxValue"
	ctx := context.WithValue(context.Background(), ctxKey{}, ctxValue)

	expectedInto := "expectedInto"

	handleErr := errors.New("handleErr")

	for _, consumeErr := range []error{
		nil,
		errors.New("consumeErr"),
	} {
		for _, c := range []struct {
			handleErr   error
			expectedAck pgqueue.Ack
			expectedErr error
		}{{
			handleErr:   nil,
			expectedAck: pgqueue.OK,
			expectedErr: consumeErr,
		}, {
			handleErr:   handleErr,
			expectedAck: pgqueue.Requeue,
			expectedErr: handleErr,
		}} {
			t.Run(fmt.Sprintf("consumeErr=%v; handleErr=%v", consumeErr, c.handleErr), func(t *testing.T) {
				consume := func(ctx context.Context, getHandler pgqueue.GetHandler) error {
					assert.Equal(t, ctxValue, ctx.Value(ctxKey{}))

					into, handle := getHandler()
					assert.Equal(t, expectedInto, into)

					ctx, ack := handle(ctx)
					assert.Equal(t, "newCtxValue", ctx.Value(ctxKey{}))
					assert.Equal(t, c.expectedAck, ack)

					return consumeErr
				}

				err := RequeueOnError(consume, ctx, func() (into interface{}, handle HandleErrFunc) {
					return expectedInto, func(ctx context.Context) (context.Context, error) {
						return context.WithValue(ctx, ctxKey{}, "newCtxValue"), c.handleErr
					}
				})

				assert.Equal(t, c.expectedErr, err)
			})
		}
	}
}
