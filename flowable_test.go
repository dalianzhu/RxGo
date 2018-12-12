package rxgo

import (
	"fmt"
	"github.com/reactivex/rxgo/handlers"
	"github.com/reactivex/rxgo/options"
	"testing"
	"time"
)

func TestFlowable(t *testing.T) {
	Just(1, 2, 3).
		ToFlowable(options.WithDropBackpressureStrategy()).
		Map(func(i interface{}) interface{} {
			return i.(int) + 10
		}).Subscribe(handlers.NextFunc(func(i interface{}) {
		fmt.Printf("%v\n", i)
	}))
	time.Sleep(1 * time.Second)

}
