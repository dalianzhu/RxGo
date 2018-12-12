package rxgo

import (
	"github.com/reactivex/rxgo/handlers"
	"github.com/reactivex/rxgo/options"
)

type Flowable interface {
	Iterable
	All(predicate Predicate) Single
	AverageFloat32() Single
	AverageFloat64() Single
	AverageInt() Single
	AverageInt8() Single
	AverageInt16() Single
	AverageInt32() Single
	AverageInt64() Single
	BufferWithCount(count, skip int) Flowable
	BufferWithTime(timespan, timeshift Duration) Flowable
	BufferWithTimeOrCount(timespan Duration, count int) Flowable
	Contains(equal Predicate) Single
	Count() Single
	DefaultIfEmpty(defaultValue interface{}) Flowable
	Distinct(apply Function) Flowable
	DistinctUntilChanged(apply Function) Flowable
	DoOnEach(onNotification Consumer) Flowable
	ElementAt(index uint) Single
	Filter(apply Predicate) Flowable
	First() Flowable
	FirstOrDefault(defaultValue interface{}) Single
	ForEach(nextFunc handlers.NextFunc, errFunc handlers.ErrFunc,
		doneFunc handlers.DoneFunc, opts ...options.Option) Observer
	Last() Flowable
	LastOrDefault(defaultValue interface{}) Single
	Map(apply Function) Flowable
	Max(comparator Comparator) OptionalSingle
	Min(comparator Comparator) OptionalSingle
	Reduce(apply Function2) OptionalSingle
	Repeat(count int64, frequency Duration) Flowable
	Scan(apply Function2) Flowable
	Skip(nth uint) Flowable
	SkipLast(nth uint) Flowable
	SkipWhile(apply Predicate) Flowable
	StartWithItems(items ...interface{}) Flowable
	StartWithIterable(iterable Iterable) Flowable
	StartWithObservable(observable Observable) Flowable
	Subscribe(handler handlers.EventHandler) Observer
	SumFloat32() Single
	SumFloat64() Single
	SumInt64() Single
	Take(nth uint) Flowable
	TakeLast(nth uint) Flowable
	TakeWhile(apply Predicate) Flowable
	ToList() Flowable
	ToMap(keySelector Function) Flowable
	ToMapWithValueSelector(keySelector Function, valueSelector Function) Flowable
	ZipFromObservable(publisher Observable, zipper Function2) Flowable
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

func (o *flowable) All(predicate Predicate) Single {
	return newColdSingle(all(o.iterable, predicate))
}

func (o *flowable) AverageFloat32() Single {
	return newColdSingle(averageFloat32(o.iterable))
}

func (o *flowable) AverageFloat64() Single {
	return newColdSingle(averageFloat64(o.iterable))
}

func (o *flowable) AverageInt() Single {
	return newColdSingle(averageInt(o.iterable))
}

func (o *flowable) AverageInt8() Single {
	return newColdSingle(averageInt8(o.iterable))
}

func (o *flowable) AverageInt16() Single {
	return newColdSingle(averageInt16(o.iterable))
}

func (o *flowable) AverageInt32() Single {
	return newColdSingle(averageInt32(o.iterable))
}

func (o *flowable) AverageInt64() Single {
	return newColdSingle(averageInt64(o.iterable))
}

func (o *flowable) BufferWithCount(count, skip int) Flowable {
	return newFlowableFromFunc(bufferWithCount(o.iterable, count, skip),
		o.backpressureStrategy, o.buffer)
}

func (o *flowable) BufferWithTime(timespan, timeshift Duration) Flowable {
	return newFlowableFromFunc(bufferWithTime(o.iterable, timespan, timeshift),
		o.backpressureStrategy, o.buffer)
}

func (o *flowable) BufferWithTimeOrCount(timespan Duration, count int) Flowable {
	return newFlowableFromFunc(bufferWithTimeOrCount(o.iterable, timespan, count),
		o.backpressureStrategy, o.buffer)
}

func (o *flowable) Contains(equal Predicate) Single {
	return newColdSingle(contains(o.iterable, equal))
}

func (o *flowable) Count() Single {
	return newColdSingle(count(o.iterable))
}

func (o *flowable) DefaultIfEmpty(defaultValue interface{}) Flowable {
	return newFlowableFromFunc(defaultIfEmpty(o.iterable, defaultValue),
		o.backpressureStrategy, o.buffer)
}

func (o *flowable) Distinct(apply Function) Flowable {
	return newFlowableFromFunc(distinct(o.iterable, apply), o.backpressureStrategy, o.buffer)
}

func (o *flowable) DistinctUntilChanged(apply Function) Flowable {
	return newFlowableFromFunc(distinctUntilChanged(o.iterable, apply), o.backpressureStrategy, o.buffer)
}

func (o *flowable) DoOnEach(onNotification Consumer) Flowable {
	return newFlowableFromFunc(doOnEach(o.iterable, onNotification), o.backpressureStrategy, o.buffer)
}

func (o *flowable) ElementAt(index uint) Single {
	return newColdSingle(elementAt(o.iterable, index))
}

func (o *flowable) Filter(apply Predicate) Flowable {
	return newFlowableFromFunc(filter(o.iterable, apply), o.backpressureStrategy, o.buffer)
}

func (o *flowable) First() Flowable {
	return newFlowableFromFunc(first(o.iterable), o.backpressureStrategy, o.buffer)
}

func (o *flowable) FirstOrDefault(defaultValue interface{}) Single {
	return newColdSingle(firstOrDefault(o.iterable, defaultValue))
}

func (o *flowable) ForEach(nextFunc handlers.NextFunc, errFunc handlers.ErrFunc,
	doneFunc handlers.DoneFunc, opts ...options.Option) Observer {
	return o.Subscribe(CheckEventHandlers(nextFunc, errFunc, doneFunc))
}

func (o *flowable) Iterator() Iterator {
	return o.iterable.Iterator()
}

func (o *flowable) Last() Flowable {
	return newFlowableFromFunc(last(o.iterable), o.backpressureStrategy, o.buffer)
}

func (o *flowable) LastOrDefault(defaultValue interface{}) Single {
	return newColdSingle(lastOrDefault(o.iterable, defaultValue))
}

func (o *flowable) Map(apply Function) Flowable {
	return newFlowableFromFunc(mapFromFunction(o.iterable, apply), o.backpressureStrategy, o.buffer)
}

func (o *flowable) Max(comparator Comparator) OptionalSingle {
	return newColdSingle(max(o.iterable, comparator))
}

func (o *flowable) Min(comparator Comparator) OptionalSingle {
	return newColdSingle(min(o.iterable, comparator))
}

func (o *flowable) Reduce(apply Function2) OptionalSingle {
	return newColdSingle(reduce(o.iterable, apply))
}

func (o *flowable) Repeat(count int64, frequency Duration) Flowable {
	return newFlowableFromFunc(repeat(o.iterable, count, frequency), o.backpressureStrategy, o.buffer)
}

func (o *flowable) Scan(apply Function2) Flowable {
	return newFlowableFromFunc(scan(o.iterable, apply), o.backpressureStrategy, o.buffer)
}

func (o *flowable) Skip(nth uint) Flowable {
	return newFlowableFromFunc(skip(o.iterable, nth), o.backpressureStrategy, o.buffer)
}

func (o *flowable) SkipLast(nth uint) Flowable {
	return newFlowableFromFunc(skipLast(o.iterable, nth), o.backpressureStrategy, o.buffer)
}

func (o *flowable) SkipWhile(apply Predicate) Flowable {
	return newFlowableFromFunc(skipWhile(o.iterable, apply), o.backpressureStrategy, o.buffer)
}

func (o *flowable) StartWithItems(items ...interface{}) Flowable {
	return newFlowableFromFunc(startWithItems(o.iterable, items...), o.backpressureStrategy, o.buffer)
}

func (o *flowable) StartWithIterable(iterable Iterable) Flowable {
	return newFlowableFromFunc(startWithIterable(o.iterable, iterable), o.backpressureStrategy, o.buffer)
}

func (o *flowable) StartWithObservable(observable Observable) Flowable {
	return newFlowableFromFunc(startWithObservable(o.iterable, observable), o.backpressureStrategy, o.buffer)
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

func (o *flowable) SumFloat32() Single {
	return newColdSingle(sumFloat32(o.iterable))
}

func (o *flowable) SumFloat64() Single {
	return newColdSingle(sumFloat64(o.iterable))
}

func (o *flowable) SumInt64() Single {
	return newColdSingle(sumInt64(o.iterable))
}

func (o *flowable) Take(nth uint) Flowable {
	return newFlowableFromFunc(take(o.iterable, nth), o.backpressureStrategy, o.buffer)
}

func (o *flowable) TakeLast(nth uint) Flowable {
	return newFlowableFromFunc(takeLast(o.iterable, nth), o.backpressureStrategy, o.buffer)
}

func (o *flowable) TakeWhile(apply Predicate) Flowable {
	return newFlowableFromFunc(takeWhile(o.iterable, apply), o.backpressureStrategy, o.buffer)
}

func (o *flowable) ToList() Flowable {
	return newFlowableFromFunc(toList(o.iterable), o.backpressureStrategy, o.buffer)
}

func (o *flowable) ToMap(keySelector Function) Flowable {
	return newFlowableFromFunc(toMap(o.iterable, keySelector), o.backpressureStrategy, o.buffer)
}

func (o *flowable) ToMapWithValueSelector(keySelector Function, valueSelector Function) Flowable {
	return newFlowableFromFunc(toMapWithValueSelector(o.iterable, keySelector, valueSelector), o.backpressureStrategy, o.buffer)
}

func (o *flowable) ZipFromObservable(publisher Observable, zipper Function2) Flowable {
	return newFlowableFromFunc(zipFromObservable(o.iterable, publisher, zipper), o.backpressureStrategy, o.buffer)
}
