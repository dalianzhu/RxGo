package rxgo

import (
	"github.com/reactivex/rxgo/handlers"
	"github.com/reactivex/rxgo/options"
	"sync"
)

type ConnectableObservable interface {
	Iterable
	All(predicate Predicate) Single
	AverageFloat32() Single
	AverageFloat64() Single
	AverageInt() Single
	AverageInt8() Single
	AverageInt16() Single
	AverageInt32() Single
	AverageInt64() Single
	BufferWithCount(count, skip int) ConnectableObservable
	BufferWithTime(timespan, timeshift Duration) ConnectableObservable
	BufferWithTimeOrCount(timespan Duration, count int) ConnectableObservable
	Connect() Observer
	Contains(equal Predicate) Single
	Count() Single
	DefaultIfEmpty(defaultValue interface{}) ConnectableObservable
	Distinct(apply Function) ConnectableObservable
	DistinctUntilChanged(apply Function) ConnectableObservable
	DoOnEach(onNotification Consumer) ConnectableObservable
	ElementAt(index uint) Single
	Filter(apply Predicate) ConnectableObservable
	First() ConnectableObservable
	FirstOrDefault(defaultValue interface{}) Single
	ForEach(nextFunc handlers.NextFunc, errFunc handlers.ErrFunc,
		doneFunc handlers.DoneFunc, opts ...options.Option) Observer
	Last() ConnectableObservable
	LastOrDefault(defaultValue interface{}) Single
	Map(Function) ConnectableObservable
	Max(comparator Comparator) OptionalSingle
	Min(comparator Comparator) OptionalSingle
	Reduce(apply Function2) OptionalSingle
	Repeat(count int64, frequency Duration) ConnectableObservable
	Scan(apply Function2) ConnectableObservable
	Skip(nth uint) ConnectableObservable
	SkipLast(nth uint) ConnectableObservable
	SkipWhile(apply Predicate) ConnectableObservable
	StartWithItems(items ...interface{}) ConnectableObservable
	StartWithIterable(iterable Iterable) ConnectableObservable
	StartWithObservable(observable Observable) ConnectableObservable
	Subscribe(handler handlers.EventHandler, opts ...options.Option) Observer
	SumFloat32() Single
	SumFloat64() Single
	SumInt64() Single
	Take(nth uint) ConnectableObservable
	TakeLast(nth uint) ConnectableObservable
	TakeWhile(apply Predicate) ConnectableObservable
	ToList() ConnectableObservable
	ToMap(keySelector Function) ConnectableObservable
	ToMapWithValueSelector(keySelector Function, valueSelector Function) ConnectableObservable
	ZipFromObservable(publisher Observable, zipper Function2) ConnectableObservable
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

func (o *connectableObservable) All(predicate Predicate) Single {
	return newColdSingle(all(o.iterable, predicate))
}

func (o *connectableObservable) AverageFloat32() Single {
	return newColdSingle(averageFloat32(o.iterable))
}

func (o *connectableObservable) AverageFloat64() Single {
	return newColdSingle(averageFloat64(o.iterable))
}

func (o *connectableObservable) AverageInt() Single {
	return newColdSingle(averageInt(o.iterable))
}

func (o *connectableObservable) AverageInt8() Single {
	return newColdSingle(averageInt8(o.iterable))
}

func (o *connectableObservable) AverageInt16() Single {
	return newColdSingle(averageInt16(o.iterable))
}

func (o *connectableObservable) AverageInt32() Single {
	return newColdSingle(averageInt32(o.iterable))
}

func (o *connectableObservable) AverageInt64() Single {
	return newColdSingle(averageInt64(o.iterable))
}

func (o *connectableObservable) BufferWithCount(count, skip int) ConnectableObservable {
	return newConnectableObservableFromFunc(bufferWithCount(o.iterable, count, skip))
}

func (o *connectableObservable) BufferWithTime(timespan, timeshift Duration) ConnectableObservable {
	return newConnectableObservableFromFunc(bufferWithTime(o.iterable, timespan, timeshift))
}

func (o *connectableObservable) BufferWithTimeOrCount(timespan Duration, count int) ConnectableObservable {
	return newConnectableObservableFromFunc(bufferWithTimeOrCount(o.iterable, timespan, count))
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

func (o *connectableObservable) Contains(equal Predicate) Single {
	return newColdSingle(contains(o.iterable, equal))
}

func (o *connectableObservable) Count() Single {
	return newColdSingle(count(o.iterable))
}

func (o *connectableObservable) DefaultIfEmpty(defaultValue interface{}) ConnectableObservable {
	return newConnectableObservableFromFunc(defaultIfEmpty(o.iterable, defaultValue))
}

func (o *connectableObservable) Distinct(apply Function) ConnectableObservable {
	return newConnectableObservableFromFunc(distinct(o.iterable, apply))
}

func (o *connectableObservable) DistinctUntilChanged(apply Function) ConnectableObservable {
	return newConnectableObservableFromFunc(distinct(o.iterable, apply))
}

func (o *connectableObservable) DoOnEach(onNotification Consumer) ConnectableObservable {
	return newConnectableObservableFromFunc(doOnEach(o.iterable, onNotification))
}

func (o *connectableObservable) ElementAt(index uint) Single {
	return newColdSingle(elementAt(o.iterable, index))
}

func (o *connectableObservable) Filter(apply Predicate) ConnectableObservable {
	return newConnectableObservableFromFunc(filter(o.iterable, apply))
}

func (o *connectableObservable) First() ConnectableObservable {
	return newConnectableObservableFromFunc(first(o.iterable))
}

func (o *connectableObservable) FirstOrDefault(defaultValue interface{}) Single {
	return newColdSingle(firstOrDefault(o.iterable, defaultValue))
}

func (o *connectableObservable) ForEach(nextFunc handlers.NextFunc, errFunc handlers.ErrFunc,
	doneFunc handlers.DoneFunc, opts ...options.Option) Observer {
	return o.Subscribe(CheckEventHandlers(nextFunc, errFunc, doneFunc))
}

func (o *connectableObservable) Iterator() Iterator {
	return o.iterable.Iterator()
}

func (o *connectableObservable) Last() ConnectableObservable {
	return newConnectableObservableFromFunc(last(o.iterable))
}

func (o *connectableObservable) LastOrDefault(defaultValue interface{}) Single {
	return newColdSingle(lastOrDefault(o.iterable, defaultValue))
}

func (o *connectableObservable) Map(apply Function) ConnectableObservable {
	return newConnectableObservableFromFunc(mapFromFunction(o.iterable, apply))
}

func (o *connectableObservable) Max(comparator Comparator) OptionalSingle {
	return newColdSingle(max(o.iterable, comparator))
}

func (o *connectableObservable) Min(comparator Comparator) OptionalSingle {
	return newColdSingle(min(o.iterable, comparator))
}

func (o *connectableObservable) Reduce(apply Function2) OptionalSingle {
	return newColdSingle(reduce(o.iterable, apply))
}

func (o *connectableObservable) Repeat(count int64, frequency Duration) ConnectableObservable {
	return newConnectableObservableFromFunc(repeat(o.iterable, count, frequency))
}

func (o *connectableObservable) Scan(apply Function2) ConnectableObservable {
	return newConnectableObservableFromFunc(scan(o.iterable, apply))
}

func (o *connectableObservable) Skip(nth uint) ConnectableObservable {
	return newConnectableObservableFromFunc(skip(o.iterable, nth))
}

func (o *connectableObservable) SkipLast(nth uint) ConnectableObservable {
	return newConnectableObservableFromFunc(skipLast(o.iterable, nth))
}

func (o *connectableObservable) SkipWhile(apply Predicate) ConnectableObservable {
	return newConnectableObservableFromFunc(skipWhile(o.iterable, apply))
}

func (o *connectableObservable) StartWithItems(items ...interface{}) ConnectableObservable {
	return newConnectableObservableFromFunc(startWithItems(o.iterable, items...))
}

func (o *connectableObservable) StartWithIterable(iterable Iterable) ConnectableObservable {
	return newConnectableObservableFromFunc(startWithIterable(o.iterable, iterable))
}

func (o *connectableObservable) StartWithObservable(observable Observable) ConnectableObservable {
	return newConnectableObservableFromFunc(startWithObservable(o.iterable, observable))
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

func (o *connectableObservable) SumFloat32() Single {
	return newColdSingle(sumFloat32(o.iterable))
}

func (o *connectableObservable) SumFloat64() Single {
	return newColdSingle(sumFloat64(o.iterable))
}

func (o *connectableObservable) SumInt64() Single {
	return newColdSingle(sumInt64(o.iterable))
}

func (o *connectableObservable) Take(nth uint) ConnectableObservable {
	return newConnectableObservableFromFunc(take(o.iterable, nth))
}

func (o *connectableObservable) TakeLast(nth uint) ConnectableObservable {
	return newConnectableObservableFromFunc(takeLast(o.iterable, nth))
}

func (o *connectableObservable) TakeWhile(apply Predicate) ConnectableObservable {
	return newConnectableObservableFromFunc(takeWhile(o.iterable, apply))
}

func (o *connectableObservable) ToList() ConnectableObservable {
	return newConnectableObservableFromFunc(toList(o.iterable))
}

func (o *connectableObservable) ToMap(keySelector Function) ConnectableObservable {
	return newConnectableObservableFromFunc(toMap(o.iterable, keySelector))
}

func (o *connectableObservable) ToMapWithValueSelector(keySelector Function, valueSelector Function) ConnectableObservable {
	return newConnectableObservableFromFunc(toMapWithValueSelector(o.iterable, keySelector, valueSelector))
}

func (o *connectableObservable) ZipFromObservable(publisher Observable, zipper Function2) ConnectableObservable {
	return newConnectableObservableFromFunc(zipFromObservable(o.iterable, publisher, zipper))
}
