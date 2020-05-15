package dispatch

import (
	"context"
	"fmt"
	"sync"
)

type FeederFunc func(chan<- Task, <-chan struct{})
type ErrFunc func(error) bool
type WorkerDebugFunc func(int, int)

type Config struct {
	Workers int

	FeederFunc
	ErrFunc
}

func (c Config) Validate() error {
	if c.Workers < 1 {
		return fmt.Errorf("Invalid dispatcher worker count %d, should be >= 1", c.Workers)
	}
	if c.FeederFunc == nil {
		return fmt.Errorf("Feeder function missing work dispatcher")
	}
	if c.ErrFunc == nil {
		return fmt.Errorf("Dispatcher error handling function missing")
	}
	return nil
}

type Task func(int, int, context.Context) error

func RunSync(c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	var wg sync.WaitGroup
	taskCh := make(chan Task, 0)
	stopCh := make(chan struct{}, c.Workers)
	go func(fn FeederFunc) {
		wg.Add(1)
		defer close(taskCh)
		defer wg.Done()
		fn(taskCh, stopCh)
	}(c.FeederFunc)

	for i := 0; i < c.Workers; i++ {
		wg.Add(1)
		go func(id int, fn ErrFunc) {
			defer wg.Done()
			var count int
			for task := range taskCh {
				if err := task(id, count, context.TODO()); err != nil {
					if c.ErrFunc != nil {
						if stop := fn(err); stop {
							stopCh <- struct{}{}
						}
					}
				}
				count++
			}
		}(i, c.ErrFunc)
	}
	wg.Wait()
	return nil
}
