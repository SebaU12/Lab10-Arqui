package main

import (
	"math"
	"sort"
	"sync"
	"time"
)

// MetricsSnapshot is an immutable copy of the measurements accumulated for
// one architecture. Latencies contains client request latency, not individual
// service latency.
type MetricsSnapshot struct {
	Architecture Architecture
	Latencies    []time.Duration

	ClientRequestsReceived  int
	ClientRequestsCompleted int
	ClientRequestsTimedOut  int

	AttemptsDispatched           int
	AttemptsStarted              int
	AttemptsCompleted            int
	AttemptsCancelledBeforeStart int
	AttemptsCancelledAfterStart  int
	RescueCandidates             int
	RescuesActivated             int
	RescuesSkippedNoCapacity     int
	RescuesSuccessful            int

	P50            time.Duration
	P95            time.Duration
	P99            time.Duration
	HasPercentiles bool
}

// MetricsCollector serializes updates made by concurrent client requests.
type MetricsCollector struct {
	mu           sync.Mutex
	architecture Architecture
	latencies    []time.Duration

	clientRequestsReceived  int
	clientRequestsCompleted int
	clientRequestsTimedOut  int

	attemptsDispatched           int
	attemptsStarted              int
	attemptsCompleted            int
	attemptsCancelledBeforeStart int
	attemptsCancelledAfterStart  int
	rescueCandidates             int
	rescuesActivated             int
	rescuesSkippedNoCapacity     int
	rescuesSuccessful            int
}

func NewMetricsCollector(architecture Architecture) *MetricsCollector {
	return &MetricsCollector{architecture: architecture}
}

// Record adds one terminal client execution and all of its terminal attempts.
func (c *MetricsCollector) Record(execution RequestExecution) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clientRequestsReceived++
	c.latencies = append(c.latencies, execution.Response.Elapsed)
	switch execution.Response.Status {
	case RequestStatusCompleted:
		c.clientRequestsCompleted++
	case RequestStatusTimedOut:
		c.clientRequestsTimedOut++
	}
	c.rescueCandidates += execution.Rescues.Candidates
	c.rescuesActivated += execution.Rescues.Activated
	c.rescuesSkippedNoCapacity += execution.Rescues.SkippedNoCapacity
	c.rescuesSuccessful += execution.Rescues.Successful

	for _, attempt := range execution.Attempts {
		c.attemptsDispatched++
		switch attempt.Status {
		case AttemptStatusCompleted:
			c.attemptsStarted++
			c.attemptsCompleted++
		case AttemptStatusCancelledBeforeStart:
			c.attemptsCancelledBeforeStart++
		case AttemptStatusCancelledAfterStart:
			c.attemptsStarted++
			c.attemptsCancelledAfterStart++
		}
	}
}

// Snapshot returns a self-contained copy that callers can inspect without
// holding the collector lock. Percentiles include completed and timed-out
// client requests as required by the experimental design.
func (c *MetricsCollector) Snapshot() MetricsSnapshot {
	c.mu.Lock()
	snapshot := MetricsSnapshot{
		Architecture:                 c.architecture,
		Latencies:                    append([]time.Duration(nil), c.latencies...),
		ClientRequestsReceived:       c.clientRequestsReceived,
		ClientRequestsCompleted:      c.clientRequestsCompleted,
		ClientRequestsTimedOut:       c.clientRequestsTimedOut,
		AttemptsDispatched:           c.attemptsDispatched,
		AttemptsStarted:              c.attemptsStarted,
		AttemptsCompleted:            c.attemptsCompleted,
		AttemptsCancelledBeforeStart: c.attemptsCancelledBeforeStart,
		AttemptsCancelledAfterStart:  c.attemptsCancelledAfterStart,
		RescueCandidates:             c.rescueCandidates,
		RescuesActivated:             c.rescuesActivated,
		RescuesSkippedNoCapacity:     c.rescuesSkippedNoCapacity,
		RescuesSuccessful:            c.rescuesSuccessful,
	}
	c.mu.Unlock()

	var ok bool
	snapshot.P50, ok = NearestRank(snapshot.Latencies, 0.50)
	snapshot.HasPercentiles = ok
	snapshot.P95, _ = NearestRank(snapshot.Latencies, 0.95)
	snapshot.P99, _ = NearestRank(snapshot.Latencies, 0.99)
	return snapshot
}

// NearestRank calculates ceil(percentile*N)-1 after sorting a copy of the
// samples. A false result means that the input is empty or percentile is
// outside (0, 1].
func NearestRank(samples []time.Duration, percentile float64) (time.Duration, bool) {
	if len(samples) == 0 || math.IsNaN(percentile) || percentile <= 0 || percentile > 1 {
		return 0, false
	}

	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left] < ordered[right]
	})
	index := int(math.Ceil(percentile*float64(len(ordered)))) - 1
	return ordered[index], true
}
