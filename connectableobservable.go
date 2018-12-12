package rxgo

import (
	"github.com/reactivex/rxgo/handlers"
	"github.com/reactivex/rxgo/options"
	"sync"
)

type ConnectableObservable interface {
	Iterable
	Connect() Observer
	Map(Function) ConnectableObservable
	Subscribe(handler handlers.EventHandler, opts ...options.Option) Observer
}

type connectableObservable struct {
	iterable       Iterable
	observersMutex sync.Mutex
	observers      []Observer
}

func newConnectableObservableFromIterable(iterable Iterable) ConnectableObservable {
	return &connectableObservable{
		iterable: iterable,
	}
}

func newConnectableObservableFromFunc(f func(chan interface{})) ConnectableObservable {
	return &connectableObservable{
		iterable: newIterableFromFunc(f),
	}
}

func (o *connectableObservable) Iterator() Iterator {
	return o.iterable.Iterator()
}

func (o *connectableObservable) Subscribe(handler handlers.EventHandler, opts ...options.Option) Observer {
	observableOptions := options.ParseOptions(opts...)

	ob := CheckEventHandler(handler)
	ob.setBackpressureStrategy(observableOptions.BackpressureStrategy())
	var ch chan interface{}
	if observableOptions.BackpressureStrategy() == options.Buffer {
		ch = make(chan interface{}, observableOptions.Buffer())
	} else {
		ch = make(chan interface{})
	}
	ob.setChannel(ch)
	o.observersMutex.Lock()
	o.observers = append(o.observers, ob)
	o.observersMutex.Unlock()

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

	return ob
}

func (o *connectableObservable) Map(apply Function) ConnectableObservable {
	return newConnectableObservableFromFunc(mapFromFunction(o.iterable, apply))
}

func (o *connectableObservable) Connect() Observer {
	out := NewObserver()
	go func() {
		it := o.iterable.Iterator()
		for {
			if item, err := it.Next(); err == nil {
				o.observersMutex.Lock()
				for _, observer := range o.observers {
					o.observersMutex.Unlock()
					select {
					case observer.getChannel() <- item:
					default:
					}
					o.observersMutex.Lock()
				}
				o.observersMutex.Unlock()
			} else {
				break
			}
		}
	}()
	return out
}
