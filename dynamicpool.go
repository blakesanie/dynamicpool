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
	workerCount   float64 // ideal count, not just actual
	direction     int
	completedJobs int64
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
	lastEval        evaluationState
}

// New creates a dynamic worker pool using an externally provided job channel.
func New(jobQueue chan Task, initialWorkers int, evaluationInterval time.Duration, scalingFactor float64) *DynamicPool {
	p := &DynamicPool{
		jobQueue:        jobQueue,
		scalingFactor:   scalingFactor,
		interval:        evaluationInterval,
		controlStopChan: make(chan struct{}),
		workers:         make([]*worker, 0, initialWorkers),
		lastEval: evaluationState{
			workerCount:   float64(initialWorkers),
			direction:     1,
			completedJobs: 0,
		},
	}

	for i := 0; i < initialWorkers; i++ {
		p.spawnWorker()
	}

	go p.controlLoop()
	return p
}

func (p *DynamicPool) spawnWorker() {
	w := &worker{
		id:       len(p.workers), // ID is just current slice length
		killChan: make(chan struct{}),
		pool:     p,
	}

	p.mu.Lock()
	p.workers = append(p.workers, w)
	p.mu.Unlock()

	p.wg.Add(1)
	go w.run()

	fmt.Printf("Spawned worker %d (total: %d)\n", w.id, len(p.workers))
}

func (p *DynamicPool) removeWorker() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.workers) == 0 {
		return
	}
	w := p.workers[len(p.workers)-1]
	p.workers = p.workers[:len(p.workers)-1]
	close(w.killChan)

	fmt.Printf("Removed worker %d (total: %d)\n", w.id, len(p.workers))
}

func (p *DynamicPool) controlLoop() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.controlStopChan:
			return
		case <-ticker.C:
			nowCompleted := atomic.LoadInt64(&p.completedJobs)
			completedThisInterval := nowCompleted - p.lastEval.completedJobs

			// Decide direction based on change in throughput
			newDirection := p.lastEval.direction
			if completedThisInterval < p.lastEval.completedJobs {
				newDirection *= -1
			}

			// Adjust float worker count
			p.lastEval.workerCount += p.scalingFactor * float64(newDirection)
			if p.lastEval.workerCount < 0 {
				p.lastEval.workerCount = 0
			}

			// Spawn/kill to match floored count
			p.mu.Lock()
			target := int(math.Floor(p.lastEval.workerCount))
			current := len(p.workers)
			if target > current {
				for i := 0; i < target-current; i++ {
					p.spawnWorker()
				}
			} else if target < current {
				for i := 0; i < current-target; i++ {
					p.removeWorker()
				}
			}
			p.mu.Unlock()

			// Save state
			p.lastEval.direction = newDirection
			p.lastEval.completedJobs = nowCompleted
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
