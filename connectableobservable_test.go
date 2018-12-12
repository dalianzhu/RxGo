package rxgo

//func TestConnect(t *testing.T) {
//	obs := Just(1, 2, 3).Publish()
//	got1 := make([]interface{}, 0)
//
//	obs.Subscribe(handlers.NextFunc(func(i interface{}) {
//		got1 = append(got1, i)
//		time.Sleep(200 * time.Millisecond) // Pause the observer on purpose
//	}), options.WithBufferBackpressureStrategy(1))
//	obs.Connect()
//
//	time.Sleep(500 * time.Millisecond) // Ugly wait just for the example
//	fmt.Printf("%v\n", got1)
//
//	obs = Just(1, 2, 3).Publish()
//	got1 = make([]interface{}, 0)
//
//	obs.Subscribe(handlers.NextFunc(func(i interface{}) {
//		got1 = append(got1, i)
//		time.Sleep(200 * time.Millisecond) // Pause the observer on purpose
//	}), options.WithDropBackpressureStrategy())
//	time.Sleep(500 * time.Millisecond) // Ugly wait just for the example
//	obs.Connect()
//
//	time.Sleep(500 * time.Millisecond) // Ugly wait just for the example
//	fmt.Printf("%v\n", got1)
//}

//func TestConnect2(t *testing.T) {
//	obs := Just(1, 2, 3).Map(func(i interface{}) interface{} {
//		return 1 + i.(int)
//	}).Publish()
//	got1 := make([]interface{}, 0)
//
//	obs.Subscribe(handlers.NextFunc(func(i interface{}) {
//		got1 = append(got1, i)
//		time.Sleep(200 * time.Millisecond) // Pause the observer on purpose
//	}), options.WithBufferBackpressureStrategy(1))
//	obs.Connect()
//
//	time.Sleep(500 * time.Millisecond) // Ugly wait just for the example
//	fmt.Printf("%v\n", got1)
//}
