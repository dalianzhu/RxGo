package rxgo

import (
	"fmt"
	"github.com/reactivex/rxgo/handlers"
	"github.com/reactivex/rxgo/options"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	obs := Just(1, 2, 3).Publish()
	got1 := make([]interface{}, 0)

	obs.Subscribe(handlers.NextFunc(func(i interface{}) {
		got1 = append(got1, i)
		time.Sleep(200 * time.Millisecond) // Pause the observer on purpose
	}), options.WithBufferBackpressureStrategy(1))
	//}), options.WithDropBackpressureStrategy())
	obs.Connect()

	time.Sleep(500 * time.Millisecond) // Ugly wait just for the example
	fmt.Printf("%v\n", got1)
}
