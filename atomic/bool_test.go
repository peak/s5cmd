package atomic

import (
	"sync"
	"testing"
)

func TestRace(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	var atomicBool Bool
	repeat := 10000

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < repeat; i++ {
			atomicBool.Set(true)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < repeat; i++ {
			_ = atomicBool.Get()
		}
	}()

	wg.Wait()
}
