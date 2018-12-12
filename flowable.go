package rxgo

import (
	"github.com/reactivex/rxgo/handlers"
	"github.com/reactivex/rxgo/options"
)

type Flowable interface {
	Iterable
	Map(apply Function) Flowable
	Subscribe(handler handlers.EventHandler) Observer
}

type flowable struct {
	iterable             Iterable
	backpressureStrategy options.BackpressureStrategy
	buffer               int
}

func newFlowableFromIterable(iterable Iterable, backpressureStrategy options.BackpressureStrategy,
	buffer int) Flowable {
	return &flowable{
		iterable:             iterable,
		backpressureStrategy: backpressureStrategy,
		buffer:               buffer,
	}
}

func newFlowableFromFunc(f func(chan interface{}), backpressureStrategy options.BackpressureStrategy,
	buffer int) Flowable {
	return newFlowableFromIterable(newIterableFromFunc(f), backpressureStrategy, buffer)
}

func (o *flowable) Iterator() Iterator {
	return o.iterable.Iterator()
}

func (o *flowable) Map(apply Function) Flowable {
	return newFlowableFromFunc(mapFromFunction(o.iterable, apply), o.backpressureStrategy, o.buffer)
}

func (o *flowable) Subscribe(handler handlers.EventHandler) Observer {
	ob := CheckEventHandler(handler)

	ob.setBackpressureStrategy(o.backpressureStrategy)
	var ch chan interface{}
	if o.backpressureStrategy == options.Buffer {
		ch = make(chan interface{}, o.buffer)
	} else {
		ch = make(chan interface{})
	}

	ob.setChannel(ch)

	go func() {
		for item := range ch {
			switch item := item.(type) {
			case error:
				ob.OnError(item)
				return
			default:
				ob.OnNext(item)
			}
		}
	}()

	go func() {
		it := o.iterable.Iterator()
		for {
			if item, err := it.Next(); err == nil {
				select {
				case ch <- item:
				default:
				}
			} else {
				break
			}
		}
	}()

	return ob
}
