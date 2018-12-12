package rxgo

import (
	"github.com/reactivex/rxgo/handlers"
	"github.com/reactivex/rxgo/options"
)

// Observable is a basic observable interface
type Observable interface {
	Iterable
	All(predicate Predicate) Single
	AverageFloat32() Single
	AverageFloat64() Single
	AverageInt() Single
	AverageInt8() Single
	AverageInt16() Single
	AverageInt32() Single
	AverageInt64() Single
	BufferWithCount(count, skip int) Observable
	BufferWithTime(timespan, timeshift Duration) Observable
	BufferWithTimeOrCount(timespan Duration, count int) Observable
	Contains(equal Predicate) Single
	Count() Single
	DefaultIfEmpty(defaultValue interface{}) Observable
	Distinct(apply Function) Observable
	DistinctUntilChanged(apply Function) Observable
	DoOnEach(onNotification Consumer) Observable
	ElementAt(index uint) Single
	Filter(apply Predicate) Observable
	First() Observable
	FirstOrDefault(defaultValue interface{}) Single
	FlatMap(apply func(interface{}) Observable, maxInParallel uint) Observable
	ForEach(nextFunc handlers.NextFunc, errFunc handlers.ErrFunc,
		doneFunc handlers.DoneFunc, opts ...options.Option) Observer
	getOnErrorResumeNext() ErrorToObservableFunction
	getOnErrorReturn() ErrorFunction
	Last() Observable
	LastOrDefault(defaultValue interface{}) Single
	Map(apply Function) Observable
	Max(comparator Comparator) OptionalSingle
	Min(comparator Comparator) OptionalSingle
	ObserveOn(scheduler options.Scheduler) Observable
	OnErrorResumeNext(resumeSequence ErrorToObservableFunction) Observable
	OnErrorReturn(resumeFunc ErrorFunction) Observable
	Publish() ConnectableObservable
	Reduce(apply Function2) OptionalSingle
	Repeat(count int64, frequency Duration) Observable
	Scan(apply Function2) Observable
	Skip(nth uint) Observable
	SkipLast(nth uint) Observable
	SkipWhile(apply Predicate) Observable
	StartWithItems(items ...interface{}) Observable
	StartWithIterable(iterable Iterable) Observable
	StartWithObservable(observable Observable) Observable
	Subscribe(handler handlers.EventHandler, opts ...options.Option) Observer
	SumFloat32() Single
	SumFloat64() Single
	SumInt64() Single
	Take(nth uint) Observable
	TakeLast(nth uint) Observable
	TakeWhile(apply Predicate) Observable
	ToFlowable(opts ...options.Option) Flowable
	ToList() Observable
	ToMap(keySelector Function) Observable
	ToMapWithValueSelector(keySelector Function, valueSelector Function) Observable
	ZipFromObservable(publisher Observable, zipper Function2) Observable
}

// observable is a structure handling a channel of interface{} and implementing Observable
type observable struct {
	iterable            Iterable
	errorOnSubscription error
	observableFactory   func() Observable
	onErrorReturn       ErrorFunction
	onErrorResumeNext   ErrorToObservableFunction
	scheduler           options.Scheduler
}

// CheckHandler checks the underlying type of an EventHandler.
func CheckEventHandler(handler handlers.EventHandler) Observer {
	return NewObserver(handler)
}

// CheckHandler checks the underlying type of an EventHandler.
func CheckEventHandlers(handler ...handlers.EventHandler) Observer {
	return NewObserver(handler...)
}

func iterate(observable Observable, observer Observer) error {
	it := observable.Iterator()
	for {
		if item, err := it.Next(); err == nil {
			switch item := item.(type) {
			case error:
				if observable.getOnErrorReturn() != nil {
					observer.OnNext(observable.getOnErrorReturn()(item))
					// Stop the subscription
					return nil
				} else if observable.getOnErrorResumeNext() != nil {
					observable = observable.getOnErrorResumeNext()(item)
					it = observable.Iterator()
				} else {
					observer.OnError(item)
					return item
				}
			default:
				observer.OnNext(item)
			}
		} else {
			break
		}
	}
	return nil
}

func (o *observable) All(predicate Predicate) Single {
	return newColdSingle(all(o.iterable, predicate))
}

// AverageFloat32 calculates the average of numbers emitted by an Observable and emits this average float32.
func (o *observable) AverageFloat32() Single {
	return newColdSingle(averageFloat32(o.iterable))
}

// AverageFloat64 calculates the average of numbers emitted by an Observable and emits this average float64.
func (o *observable) AverageFloat64() Single {
	return newColdSingle(averageFloat64(o.iterable))
}

// AverageInt calculates the average of numbers emitted by an Observable and emits this average int.
func (o *observable) AverageInt() Single {
	return newColdSingle(averageInt(o.iterable))
}

// AverageInt8 calculates the average of numbers emitted by an Observable and emits this average int8.
func (o *observable) AverageInt8() Single {
	return newColdSingle(averageInt8(o.iterable))
}

// AverageInt16 calculates the average of numbers emitted by an Observable and emits this average int16.
func (o *observable) AverageInt16() Single {
	return newColdSingle(averageInt16(o.iterable))
}

// AverageInt32 calculates the average of numbers emitted by an Observable and emits this average int32.
func (o *observable) AverageInt32() Single {
	return newColdSingle(averageInt32(o.iterable))
}

// AverageInt64 calculates the average of numbers emitted by an Observable and emits this average int64.
func (o *observable) AverageInt64() Single {
	return newColdSingle(averageInt64(o.iterable))
}

// BufferWithCount returns an Observable that emits buffers of items it collects
// from the source Observable.
// The resulting Observable emits buffers every skip items, each containing a slice of count items.
// When the source Observable completes or encounters an error,
// the resulting Observable emits the current buffer and propagates
// the notification from the source Observable.
func (o *observable) BufferWithCount(count, skip int) Observable {
	return &observable{
		iterable: newIterableFromFunc(bufferWithCount(o.iterable, count, skip)),
	}
}

// BufferWithTime returns an Observable that emits buffers of items it collects from the source
// Observable. The resulting Observable starts a new buffer periodically, as determined by the
// timeshift argument. It emits each buffer after a fixed timespan, specified by the timespan argument.
// When the source Observable completes or encounters an error, the resulting Observable emits
// the current buffer and propagates the notification from the source Observable.
func (o *observable) BufferWithTime(timespan, timeshift Duration) Observable {
	return &observable{
		iterable: newIterableFromFunc(bufferWithTime(o.iterable, timespan, timeshift)),
	}
}

// BufferWithTimeOrCount returns an Observable that emits buffers of items it collects
// from the source Observable. The resulting Observable emits connected,
// non-overlapping buffers, each of a fixed duration specified by the timespan argument
// or a maximum size specified by the count argument (whichever is reached first).
// When the source Observable completes or encounters an error, the resulting Observable
// emits the current buffer and propagates the notification from the source Observable.
func (o *observable) BufferWithTimeOrCount(timespan Duration, count int) Observable {
	return &observable{
		iterable: newIterableFromFunc(bufferWithTimeOrCount(o.iterable, timespan, count)),
	}
}

// Contains returns an Observable that emits a Boolean that indicates whether
// the source Observable emitted an item (the comparison is made against a predicate).
func (o *observable) Contains(equal Predicate) Single {
	return newColdSingle(contains(o.iterable, equal))
}

func (o *observable) Count() Single {
	return newColdSingle(count(o.iterable))
}

// DefaultIfEmpty returns an Observable that emits the items emitted by the source
// Observable or a specified default item if the source Observable is empty.
func (o *observable) DefaultIfEmpty(defaultValue interface{}) Observable {
	return &observable{
		iterable: newIterableFromFunc(defaultIfEmpty(o.iterable, defaultValue)),
	}
}

// Distinct suppresses duplicate items in the original Observable and returns
// a new Observable.
func (o *observable) Distinct(apply Function) Observable {
	return &observable{
		iterable: newIterableFromFunc(distinct(o.iterable, apply)),
	}
}

// DistinctUntilChanged suppresses consecutive duplicate items in the original
// Observable and returns a new Observable.
func (o *observable) DistinctUntilChanged(apply Function) Observable {
	return &observable{
		iterable: newIterableFromFunc(distinctUntilChanged(o.iterable, apply)),
	}
}

// DoOnEach operator allows you to establish a callback that the resulting Observable
// will call each time it emits an item
func (o *observable) DoOnEach(onNotification Consumer) Observable {
	return &observable{
		iterable: newIterableFromFunc(doOnEach(o.iterable, onNotification)),
	}
}

func (o *observable) ElementAt(index uint) Single {
	return newColdSingle(elementAt(o.iterable, index))
}

// Filter filters items in the original Observable and returns
// a new Observable with the filtered items.
func (o *observable) Filter(apply Predicate) Observable {
	return &observable{
		iterable: newIterableFromFunc(filter(o.iterable, apply)),
	}
}

// First returns new Observable which emit only first item.
func (o *observable) First() Observable {
	return &observable{
		iterable: newIterableFromFunc(first(o.iterable)),
	}
}

// FirstOrDefault returns new Observable which emit only first item.
// If the observable fails to emit any items, it emits a default value.
func (o *observable) FirstOrDefault(defaultValue interface{}) Single {
	return newColdSingle(firstOrDefault(o.iterable, defaultValue))
}

// ForEach subscribes to the Observable and receives notifications for each element.
func (o *observable) ForEach(nextFunc handlers.NextFunc, errFunc handlers.ErrFunc,
	doneFunc handlers.DoneFunc, opts ...options.Option) Observer {
	return o.Subscribe(CheckEventHandlers(nextFunc, errFunc, doneFunc), opts...)
}

func (o *observable) getOnErrorResumeNext() ErrorToObservableFunction {
	return o.onErrorResumeNext
}

func (o *observable) getOnErrorReturn() ErrorFunction {
	return o.onErrorReturn
}

func (o *observable) Iterator() Iterator {
	return o.iterable.Iterator()
}

// Last returns a new Observable which emit only last item.
func (o *observable) Last() Observable {
	return &observable{
		iterable: newIterableFromFunc(last(o.iterable)),
	}
}

// Last returns a new Observable which emit only last item.
// If the observable fails to emit any items, it emits a default value.
func (o *observable) LastOrDefault(defaultValue interface{}) Single {
	return newColdSingle(lastOrDefault(o.iterable, defaultValue))
}

// Map maps a Function predicate to each item in Observable and
// returns a new Observable with applied items.
func (o *observable) Map(apply Function) Observable {
	return &observable{
		iterable: newIterableFromFunc(mapFromFunction(o.iterable, apply)),
	}
}

// Max determines and emits the maximum-valued item emitted by an Observable according to a comparator.
func (o *observable) Max(comparator Comparator) OptionalSingle {
	return newColdSingle(max(o.iterable, comparator))
}

// Min determines and emits the minimum-valued item emitted by an Observable according to a comparator.
func (o *observable) Min(comparator Comparator) OptionalSingle {
	return newColdSingle(min(o.iterable, comparator))
}

func (o *observable) ObserveOn(scheduler options.Scheduler) Observable {
	return &observable{
		iterable:  o.iterable,
		scheduler: scheduler,
	}
}

// OnErrorResumeNext Instructs an Observable to pass control to another Observable rather than invoking
// onError if it encounters an error.
func (o *observable) OnErrorResumeNext(resumeSequence ErrorToObservableFunction) Observable {
	o.onErrorResumeNext = resumeSequence
	o.onErrorReturn = nil
	return o
}

// OnErrorReturn instructs an Observable to emit an item (returned by a specified function)
// rather than invoking onError if it encounters an error.
func (o *observable) OnErrorReturn(resumeFunc ErrorFunction) Observable {
	o.onErrorReturn = resumeFunc
	o.onErrorResumeNext = nil
	return o
}

// Publish returns a ConnectableObservable which waits until its connect method
// is called before it begins emitting items to those Observers that have subscribed to it.
func (o *observable) Publish() ConnectableObservable {
	return newConnectableObservableFromIterable(o.iterable)
}

func (o *observable) Reduce(apply Function2) OptionalSingle {
	return newColdSingle(reduce(o.iterable, apply))
}

// Repeat returns an Observable that repeats the sequence of items emitted by the source Observable
// at most count times, at a particular frequency.
func (o *observable) Repeat(count int64, frequency Duration) Observable {
	return &observable{
		iterable: newIterableFromFunc(repeat(o.iterable, count, frequency)),
	}
}

// Scan applies Function2 predicate to each item in the original
// Observable sequentially and emits each successive value on a new Observable.
func (o *observable) Scan(apply Function2) Observable {
	return &observable{
		iterable: newIterableFromFunc(scan(o.iterable, apply)),
	}
}

// Skip suppresses the first n items in the original Observable and
// returns a new Observable with the rest items.
func (o *observable) Skip(nth uint) Observable {
	return &observable{
		iterable: newIterableFromFunc(skip(o.iterable, nth)),
	}
}

// SkipLast suppresses the last n items in the original Observable and
// returns a new Observable with the rest items.
func (o *observable) SkipLast(nth uint) Observable {
	return &observable{
		iterable: newIterableFromFunc(skipLast(o.iterable, nth)),
	}
}

// SkipWhile discard items emitted by an Observable until a specified condition becomes false.
func (o *observable) SkipWhile(apply Predicate) Observable {
	return &observable{
		iterable: newIterableFromFunc(skipWhile(o.iterable, apply)),
	}
}

// StartWithItems returns an Observable that emits the specified items before it begins to emit items emitted
// by the source Observable.
func (o *observable) StartWithItems(items ...interface{}) Observable {
	return &observable{
		iterable: newIterableFromFunc(startWithItems(o.iterable, items...)),
	}
}

// StartWithIterable returns an Observable that emits the items in a specified Iterable before it begins to
// emit items emitted by the source Observable.
func (o *observable) StartWithIterable(iterable Iterable) Observable {
	return &observable{
		iterable: newIterableFromFunc(startWithIterable(o.iterable, iterable)),
	}
}

// StartWithObservable returns an Observable that emits the items in a specified Observable before it begins to
// emit items emitted by the source Observable.
func (o *observable) StartWithObservable(obs Observable) Observable {
	return &observable{
		iterable: newIterableFromFunc(startWithObservable(o.iterable, obs)),
	}
}

// Subscribe subscribes an EventHandler and returns a Subscription channel.
func (o *observable) Subscribe(handler handlers.EventHandler, opts ...options.Option) Observer {
	ob := CheckEventHandler(handler)

	observableOptions := options.ParseOptions(opts...)

	if o.errorOnSubscription != nil {
		go func() {
			ob.OnError(o.errorOnSubscription)
		}()
		return ob
	}

	if observableOptions.Parallelism() == 0 {
		go func() {
			e := iterate(o, ob)
			if e == nil {
				ob.OnDone()
			}
		}()
	} else {
		results := make([]chan error, 0)
		for i := 0; i < observableOptions.Parallelism(); i++ {
			ch := make(chan error)
			go func() {
				ch <- iterate(o, ob)
			}()
			results = append(results, ch)
		}

		go func() {
			for _, ch := range results {
				err := <-ch
				if err != nil {
					return
				}
			}

			ob.OnDone()
		}()
	}

	return ob
}

// SumFloat32 calculates the average of float32 emitted by an Observable and emits a float32.
func (o *observable) SumFloat32() Single {
	return newColdSingle(sumFloat32(o.iterable))
}

// SumFloat64 calculates the average of float64 emitted by an Observable and emits a float64.
func (o *observable) SumFloat64() Single {
	return newColdSingle(sumFloat64(o.iterable))
}

// SumInt64 calculates the average of integers emitted by an Observable and emits an int64.
func (o *observable) SumInt64() Single {
	return newColdSingle(sumInt64(o.iterable))
}

// Take takes first n items in the original Obserable and returns
// a new Observable with the taken items.
func (o *observable) Take(nth uint) Observable {
	return &observable{
		iterable: newIterableFromFunc(take(o.iterable, nth)),
	}
}

// TakeLast takes last n items in the original Observable and returns
// a new Observable with the taken items.
func (o *observable) TakeLast(nth uint) Observable {
	return &observable{
		iterable: newIterableFromFunc(takeLast(o.iterable, nth)),
	}
}

// TakeWhile emits items emitted by an Observable as long as the
// specified condition is true, then skip the remainder.
func (o *observable) TakeWhile(apply Predicate) Observable {
	return &observable{
		iterable: newIterableFromFunc(takeWhile(o.iterable, apply)),
	}
}

func (o *observable) ToFlowable(opts ...options.Option) Flowable {
	flowableOptions := options.ParseOptions(opts...)

	return newFlowableFromIterable(o.iterable,
		flowableOptions.BackpressureStrategy(), flowableOptions.Buffer())
}

// ToList collects all items from an Observable and emit them as a single List.
func (o *observable) ToList() Observable {
	return &observable{
		iterable: newIterableFromFunc(toList(o.iterable)),
	}
}

// ToMap convert the sequence of items emitted by an Observable
// into a map keyed by a specified key function
func (o *observable) ToMap(keySelector Function) Observable {
	return &observable{
		iterable: newIterableFromFunc(toMap(o.iterable, keySelector)),
	}
}

// ToMapWithValueSelector convert the sequence of items emitted by an Observable
// into a map keyed by a specified key function and valued by another
// value function
func (o *observable) ToMapWithValueSelector(keySelector Function, valueSelector Function) Observable {
	return &observable{
		iterable: newIterableFromFunc(toMapWithValueSelector(o.iterable, keySelector, valueSelector)),
	}
}

// ZipFromObservable che emissions of multiple Observables together via a specified function
// and emit single items for each combination based on the results of this function
func (o *observable) ZipFromObservable(publisher Observable, zipper Function2) Observable {
	return &observable{
		iterable: newIterableFromFunc(zipFromObservable(o.iterable, publisher, zipper)),
	}
}
