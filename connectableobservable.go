package rxgo

import (
	"github.com/reactivex/rxgo/handlers"
	"github.com/reactivex/rxgo/options"
	"sync"
)

type ConnectableObservable interface {
	Iterable
	Connect() Observer
	Subscribe(handler handlers.EventHandler, opts ...options.Option) Observer
}

type connectableObservable struct {
	iterator       Iterator
	observable     Observable
	observersMutex sync.Mutex
	observers      []Observer
}

func newConnectableObservable(observable Observable) ConnectableObservable {
	return &connectableObservable{
		observable: observable,
		iterator:   observable.Iterator(),
	}
}

func (c *connectableObservable) Iterator() Iterator {
	return c.iterator
}

func (c *connectableObservable) Subscribe(handler handlers.EventHandler, opts ...options.Option) Observer {
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
	c.observersMutex.Lock()
	c.observers = append(c.observers, ob)
	c.observersMutex.Unlock()

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

func (c *connectableObservable) Connect() Observer {
	out := NewObserver()
	go func() {
		it := c.iterator
		for it.Next() {
			item := it.Value()
			c.observersMutex.Lock()
			for _, observer := range c.observers {
				c.observersMutex.Unlock()
				select {
				case observer.getChannel() <- item:
				default:
				}
				c.observersMutex.Lock()
			}
			c.observersMutex.Unlock()
		}
	}()
	return out
}
