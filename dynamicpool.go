package dynamicpool

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Task is an interface for units of work that the pool will execute.
type Task interface {
	Execute()
}

// evaluationState stores the last evaluation's metrics and ideal worker count.
type evaluationState struct {
	workerCount             float64 // ideal count, not just actual
	increasing              bool
	previouslyCompletedJobs int64
}

type worker struct {
	id       int
	killChan chan struct{}
	pool     *DynamicPool
}

func (w *worker) run() {
	defer w.pool.wg.Done()
	for {
		select {
		case <-w.killChan:
			return
		case task, ok := <-w.pool.jobQueue:
			if !ok {
				return
			}
			task.Execute()
			atomic.AddInt64(&w.pool.completedJobs, 1)
		}
	}
}

type DynamicPool struct {
	jobQueue        chan Task
	wg              sync.WaitGroup
	scalingFactor   float64 // can be fractional
	interval        time.Duration
	workers         []*worker
	mu              sync.Mutex
	completedJobs   int64
	controlStopChan chan struct{}
	state           evaluationState
}

// New creates a dynamic worker pool using an externally provided job channel.
func New(jobQueue chan Task, initialWorkers int, evaluationInterval time.Duration, scalingFactor float64) *DynamicPool {
	if scalingFactor <= 1 {
		panic("scalingFactor must be > 1 (ex. 1.2)")
	}
	if evaluationInterval == 0 {
		panic("evaluationInterval must be > 0")
	}
	if initialWorkers < 1 {
		initialWorkers = 1
	}
	p := &DynamicPool{
		jobQueue:        jobQueue,
		scalingFactor:   scalingFactor,
		interval:        evaluationInterval,
		controlStopChan: make(chan struct{}),
		workers:         make([]*worker, 0, initialWorkers),
		state: evaluationState{
			workerCount:             float64(initialWorkers),
			increasing:              true,
			previouslyCompletedJobs: 0,
		},
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 0; i < initialWorkers; i++ {
		p.spawnWorker()
	}
	go p.controlLoop()
	return p
}

// requires outside lock
func (p *DynamicPool) spawnWorker() {
	w := &worker{
		id:       len(p.workers), // ID is just current slice length
		killChan: make(chan struct{}),
		pool:     p,
	}

	p.workers = append(p.workers, w)

	p.wg.Add(1)
	go w.run()

	fmt.Printf("Spawned worker %d (total: %d)\n", w.id, len(p.workers))
}

// requires outside lock
func (p *DynamicPool) removeWorker() {
	if len(p.workers) == 0 {
		return
	}
	w := p.workers[len(p.workers)-1]
	p.workers = p.workers[:len(p.workers)-1]
	close(w.killChan)

	fmt.Printf("Removed worker %d (total: %d)\n", w.id, len(p.workers))
}

func (p *DynamicPool) controlLoop() {

	var lastStart time.Time
	for {
		select {
		case <-p.controlStopChan:
			return
		default:
			sinceLast := time.Since(lastStart).Milliseconds()
			fmt.Println("time since last control loop:", sinceLast)
			lastStart = time.Now()
			nowCompleted := atomic.LoadInt64(&p.completedJobs)
			atomic.StoreInt64(&p.completedJobs, 0)
			fmt.Println("jobs completed", nowCompleted)
			// Decide direction based on change in throughput
			shouldIncrease := p.state.increasing
			if nowCompleted < p.state.previouslyCompletedJobs {
				shouldIncrease = !shouldIncrease
			}

			if shouldIncrease {
				p.state.workerCount *= p.scalingFactor
			} else {
				p.state.workerCount /= p.scalingFactor
			}
			if p.state.workerCount < 1 {
				p.state.workerCount = 1
			}

			// Spawn/kill to match floored count
			p.mu.Lock()
			target := int(math.Floor(p.state.workerCount))
			current := len(p.workers)
			if target > current {
				fmt.Println("Raise workers to", target)
				for i := 0; i < target-current; i++ {
					p.spawnWorker()
				}
			} else if target < current {
				fmt.Println("Reduce workers to", target)
				for i := 0; i < current-target; i++ {
					p.removeWorker()
				}
			}
			p.mu.Unlock()

			// Save state
			p.state.increasing = shouldIncrease
			p.state.previouslyCompletedJobs = nowCompleted
			elapsed := time.Since(lastStart)
			time.Sleep(p.interval - elapsed)
		}
	}
}

// Close stops the control loop and all workers, then waits for them to finish.
func (p *DynamicPool) WaitAndClose() {
	close(p.controlStopChan)
	p.mu.Lock()
	for _, w := range p.workers {
		close(w.killChan)
	}
	p.workers = nil
	p.mu.Unlock()
	p.wg.Wait()
}
