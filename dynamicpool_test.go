package dynamicpool

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Helpers -----------------------------------------------------------------

// sleepTask is a simple Task that sleeps for duration d and increments an atomic counter.
type sleepTask struct {
	d   time.Duration
	cnt *int64
	wg  *sync.WaitGroup // optional: call Done when Execute completes
}

func (s *sleepTask) Execute() {
	time.Sleep(s.d)
	if s.cnt != nil {
		atomic.AddInt64(s.cnt, 1)
	}
	if s.wg != nil {
		s.wg.Done()
	}
}

// waitForCondition polls cond() until it returns true or timeout elapses.
// Returns true if condition met, false on timeout.
func waitForCondition(timeout time.Duration, interval time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}

// --- Tests -------------------------------------------------------------------

// TestFastTasksIncreasesWorkers verifies that under a fast task workload the pool
// ramps up its worker count (floored int) above the initial number.
func TestFastTasksIncreasesWorkers(t *testing.T) {
	// fast tasks: 10ms per task
	jobs := make(chan Task, 1000)
	initial := 1
	interval := 100 * time.Millisecond
	p := New(jobs, initial, interval, 1.0)

	// continuously produce tasks for a while
	stopProducer := make(chan struct{})
	go func() {
		for {
			select {
			case jobs <- &sleepTask{d: 10 * time.Millisecond}:
			case <-stopProducer:
				return
			}
		}
	}()

	// allow several evaluation cycles to run
	ok := waitForCondition(2*time.Second, 50*time.Millisecond, func() bool {
		p.mu.Lock()
		curr := len(p.workers)
		p.mu.Unlock()
		return curr > initial
	})

	// stop producer and close pool
	close(stopProducer)
	p.WaitAndClose()

	if !ok {
		t.Fatalf("expected worker count to increase above %d within timeout, but it did not", initial)
	}
}

// TestSlowTasksDecreasesWorkers verifies that under slow tasks the pool lowers
// the (floored) worker count below the initial number.
func TestSlowTasksDecreasesWorkers(t *testing.T) {
	// slow tasks: 300ms per task
	jobs := make(chan Task, 1000)
	initial := 6
	interval := 150 * time.Millisecond
	p := New(jobs, initial, interval, 1.0)

	// produce tasks continuously (so workers remain busy)
	stopProducer := make(chan struct{})
	go func() {
		for {
			select {
			case jobs <- &sleepTask{d: 300 * time.Millisecond}:
			case <-stopProducer:
				return
			}
		}
	}()

	// expect pool to reduce worker count after several intervals
	ok := waitForCondition(3*time.Second, 100*time.Millisecond, func() bool {
		p.mu.Lock()
		curr := len(p.workers)
		p.mu.Unlock()
		return curr < initial
	})

	close(stopProducer)
	p.WaitAndClose()

	if !ok {
		t.Fatalf("expected worker count to decrease below %d within timeout, but it did not", initial)
	}
}

// TestConcurrentSubmissionsThreadSafety submits many tasks concurrently from
// multiple goroutines and verifies all tasks execute (no lost tasks / panics).
func TestConcurrentSubmissionsThreadSafety(t *testing.T) {
	jobs := make(chan Task, 1000)
	initial := 3
	interval := 100 * time.Millisecond
	p := New(jobs, initial, interval, 1.0)

	const totalTasks = 500
	var executed int64
	var wg sync.WaitGroup
	wg.Add(totalTasks)

	// create tasks that signal wg.Done when executed
	for i := 0; i < totalTasks; i++ {
		task := &sleepTask{d: 5 * time.Millisecond, cnt: &executed, wg: &wg}
		// concurrently submit from multiple goroutines
		go func(tk Task) {
			jobs <- tk
		}(task)
	}

	// Wait for tasks to be executed (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success, all tasks done
	case <-time.After(5 * time.Second):
		p.WaitAndClose()
		t.Fatalf("timed out waiting for %d tasks to complete; completed=%d", totalTasks, atomic.LoadInt64(&executed))
	}

	// cleanup
	p.WaitAndClose()

	// final sanity check
	if atomic.LoadInt64(&executed) != totalTasks {
		t.Fatalf("expected %d executed tasks, got %d", totalTasks, atomic.LoadInt64(&executed))
	}
}

// TestFractionalScaling ensures fractional scalingFactor moves the ideal workerCount
// smoothly, and the physical worker count increases only when floor changes.
func TestFractionalScaling(t *testing.T) {
	jobs := make(chan Task, 1000)
	initial := 1
	interval := 100 * time.Millisecond
	// fractional scaling: add 0.5 ideal workers per interval in positive direction
	p := New(jobs, initial, interval, 0.5)

	// submit a steady stream of fast tasks so pool will attempt to grow
	stopProducer := make(chan struct{})
	go func() {
		for {
			select {
			case jobs <- &sleepTask{d: 10 * time.Millisecond}:
			case <-stopProducer:
				return
			}
		}
	}()

	// Wait until the floored worker count has increased above initial.
	ok := waitForCondition(2*time.Second, 50*time.Millisecond, func() bool {
		p.mu.Lock()
		curr := len(p.workers)
		ideal := p.lastEval.workerCount
		p.mu.Unlock()
		// worker should increase only when floor(ideal) > initial
		return curr > initial && int(math.Floor(ideal)) >= curr
	})

	close(stopProducer)
	p.WaitAndClose()

	if !ok {
		t.Fatalf("expected fractional scaling to produce a worker > %d within timeout", initial)
	}
}
