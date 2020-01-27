package pgqueueutil

import (
	"context"

	"gitlab.com/canastic/pgqueue"
)

type HandleErrFunc = func(context.Context) (context.Context, error)

type GetErrHandler = func() (into interface{}, handle HandleErrFunc)

func RequeueOnError(consume pgqueue.ConsumeFunc, ctx context.Context, getHandler GetErrHandler) error {
	var err error
	consumeErr := consume(ctx, func() (interface{}, pgqueue.HandleFunc) {
		into, handle := getHandler()
		return into, func(ctx context.Context) (context.Context, pgqueue.Ack) {
			ctx, err = handle(ctx)
			ack := pgqueue.OK
			if err != nil {
				ack = pgqueue.Requeue
			}
			return ctx, ack
		}
	})
	if err == nil {
		err = consumeErr
	}
	return err
}
