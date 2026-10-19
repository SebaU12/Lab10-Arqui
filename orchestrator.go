package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Architecture string

const (
	ArchitectureBaseline Architecture = "baseline"
	ArchitectureProposal Architecture = "proposal"
)

type RequestStatus string

const (
	RequestStatusReceived  RequestStatus = "received"
	RequestStatusCompleted RequestStatus = "completed"
	RequestStatusTimedOut  RequestStatus = "timed_out"
)

type ClientRequest struct {
	ID int
}

// AggregatedResponse contains at most one accepted response per logical
// service. A completed response must contain all ten services; a timed-out
// response retains the partial results for auditability.
type AggregatedResponse struct {
	RequestID  int
	Status     RequestStatus
	Results    map[ServiceID]ServiceResponse
	StartedAt  time.Time
	FinishedAt time.Time
	Elapsed    time.Duration
}

// RequestExecution contains both the client-visible aggregate and every
// primary attempt needed to audit how it was produced.
type RequestExecution struct {
	Response AggregatedResponse
	Attempts []AttemptRecord
	Rescues  RescueRequestStats
}

// PrimaryInstance models one service instance with bounded capacity. A
// goroutine is an attempt; acquiring a slot is what makes that attempt start.
type PrimaryInstance struct {
	id      string
	service LogicalService
	slots   chan struct{}

	mu            sync.Mutex
	occupancy     int
	peakOccupancy int
}

func newPrimaryInstance(service LogicalService, capacity int) *PrimaryInstance {
	return &PrimaryInstance{
		id:      fmt.Sprintf("primary-%s", service.ID),
		service: service,
		slots:   make(chan struct{}, capacity),
	}
}

func (i *PrimaryInstance) acquire(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	select {
	case i.slots <- struct{}{}:
		// A slot and cancellation can become ready at the same instant. Prefer
		// cancellation so an attempt that expired while queued is not counted
		// as started merely because select chose the slot case.
		if ctx.Err() != nil {
			<-i.slots
			return false
		}
		i.mu.Lock()
		i.occupancy++
		if i.occupancy > i.peakOccupancy {
			i.peakOccupancy = i.occupancy
		}
		i.mu.Unlock()
		return true
	case <-ctx.Done():
		return false
	}
}

func (i *PrimaryInstance) release() {
	i.mu.Lock()
	i.occupancy--
	<-i.slots
	i.mu.Unlock()
}

// Occupancy returns the current and maximum observed concurrent executions.
func (i *PrimaryInstance) Occupancy() (current, peak int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.occupancy, i.peakOccupancy
}

// BaselineOrchestrator owns ten primary instances and the immutable latency
// plan shared with the future proposal architecture.
type BaselineOrchestrator struct {
	config    Config
	plan      LatencyPlan
	instances map[ServiceID]*PrimaryInstance
	metrics   *MetricsCollector
}

func NewBaselineOrchestrator(config Config, plan LatencyPlan) (*BaselineOrchestrator, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("create baseline orchestrator: %w", err)
	}
	if plan.RequestCount() != config.RequestCount {
		return nil, fmt.Errorf(
			"create baseline orchestrator: latency plan contains %d requests, want %d",
			plan.RequestCount(),
			config.RequestCount,
		)
	}

	instances := make(map[ServiceID]*PrimaryInstance, ServiceCount)
	for _, service := range Services() {
		for requestID := 1; requestID <= config.RequestCount; requestID++ {
			if _, ok := plan.Latencies(requestID, service.ID); !ok {
				return nil, fmt.Errorf(
					"create baseline orchestrator: missing latency for request %d and service %s",
					requestID,
					service.ID,
				)
			}
		}
		instances[service.ID] = newPrimaryInstance(service, config.CapacityPerInstance)
	}

	return &BaselineOrchestrator{
		config:    config,
		plan:      plan,
		instances: instances,
		metrics:   NewMetricsCollector(ArchitectureBaseline),
	}, nil
}

type primaryAttemptOutcome struct {
	record   AttemptRecord
	response ServiceResponse
}

// Execute dispatches all ten primary attempts, accepts one valid response from
// each service, and does not return until every attempt has completed or
// acknowledged cancellation.
func (o *BaselineOrchestrator) Execute(
	ctx context.Context,
	request ClientRequest,
) (RequestExecution, error) {
	if request.ID < 1 || request.ID > o.plan.RequestCount() {
		return RequestExecution{}, fmt.Errorf(
			"execute baseline request: request ID %d outside latency plan range [1, %d]",
			request.ID,
			o.plan.RequestCount(),
		)
	}

	startedAt := time.Now()
	requestContext, cancel := context.WithTimeout(ctx, o.config.Timeout)
	defer cancel()

	plannedLatencies := make(map[ServiceID]ServiceLatencies, ServiceCount)
	for _, serviceID := range serviceIDs {
		latencies, ok := o.plan.Latencies(request.ID, serviceID)
		if !ok {
			return RequestExecution{}, fmt.Errorf(
				"execute baseline request: missing latency for request %d and service %s",
				request.ID,
				serviceID,
			)
		}
		plannedLatencies[serviceID] = latencies
	}

	outcomes := make(chan primaryAttemptOutcome, ServiceCount)
	var waitGroup sync.WaitGroup
	waitGroup.Add(ServiceCount)

	for _, serviceID := range serviceIDs {
		latency := plannedLatencies[serviceID].Primary
		attempt := Attempt{
			ID:             fmt.Sprintf("baseline-r%d-%s-primary", request.ID, serviceID),
			RequestID:      request.ID,
			ServiceID:      serviceID,
			Type:           AttemptTypePrimary,
			InstanceID:     o.instances[serviceID].id,
			PlannedLatency: latency.Duration,
			PlannedSlow:    latency.Slow,
		}
		record := AttemptRecord{
			Attempt:      attempt,
			Status:       AttemptStatusDispatched,
			DispatchedAt: time.Now(),
		}
		instance := o.instances[serviceID]
		o.config.logEvent(
			ArchitectureBaseline,
			request.ID,
			"PRIMARY_DISPATCHED",
			"service=%s instance=%s planned=%s slow=%t",
			serviceID,
			instance.id,
			latency.Duration,
			latency.Slow,
		)

		go func() {
			defer waitGroup.Done()
			outcomes <- instance.execute(requestContext, record)
		}()
	}

	response := AggregatedResponse{
		RequestID: request.ID,
		Status:    RequestStatusReceived,
		Results:   make(map[ServiceID]ServiceResponse, ServiceCount),
		StartedAt: startedAt,
	}
	records := make([]AttemptRecord, 0, ServiceCount)
	received := 0
	terminal := false

	for received < ServiceCount {
		if terminal {
			outcome := <-outcomes
			records = append(records, outcome.record)
			received++
			continue
		}

		select {
		case outcome := <-outcomes:
			received++
			if outcome.record.Status == AttemptStatusCompleted {
				outcome.record.Winner = true
				response.Results[outcome.response.ServiceID] = outcome.response
				o.config.logEvent(
					ArchitectureBaseline,
					request.ID,
					"RESULT_ACCEPTED",
					"service=%s type=primary instance=%s total=%s",
					outcome.record.Attempt.ServiceID,
					outcome.record.Attempt.InstanceID,
					outcome.record.TotalElapsed,
				)
			}
			records = append(records, outcome.record)

			if len(response.Results) == ServiceCount {
				response.Status = RequestStatusCompleted
				response.FinishedAt = time.Now()
				response.Elapsed = response.FinishedAt.Sub(response.StartedAt)
				terminal = true
				cancel()
				o.config.logEvent(
					ArchitectureBaseline,
					request.ID,
					"REQUEST_COMPLETED",
					"elapsed=%s results=%d",
					response.Elapsed,
					len(response.Results),
				)
			}
		case <-requestContext.Done():
			response.Status = RequestStatusTimedOut
			response.FinishedAt = time.Now()
			response.Elapsed = response.FinishedAt.Sub(response.StartedAt)
			terminal = true
			cancel()
		}
	}

	waitGroup.Wait()
	if !terminal {
		response.Status = RequestStatusTimedOut
		response.FinishedAt = time.Now()
		response.Elapsed = response.FinishedAt.Sub(response.StartedAt)
	}

	sort.Slice(records, func(left, right int) bool {
		return serviceOrder(records[left].Attempt.ServiceID) < serviceOrder(records[right].Attempt.ServiceID)
	})

	execution := RequestExecution{
		Response: response,
		Attempts: records,
	}
	o.metrics.Record(execution)
	return execution, nil
}

func (i *PrimaryInstance) execute(
	ctx context.Context,
	record AttemptRecord,
) primaryAttemptOutcome {
	if !i.acquire(ctx) {
		record.Status = AttemptStatusCancelledBeforeStart
		record.CancellationReason = cancellationReasonFromContext(ctx)
		record.FinishedAt = time.Now()
		record.TotalElapsed = record.FinishedAt.Sub(record.DispatchedAt)
		return primaryAttemptOutcome{record: record}
	}
	defer i.release()

	record.Status = AttemptStatusStarted
	record.StartedAt = time.Now()
	record.QueueWait = record.StartedAt.Sub(record.DispatchedAt)
	response, err := i.service.Handle(ctx, record.Attempt.RequestID, record.Attempt.PlannedLatency)
	record.FinishedAt = time.Now()
	record.ExecutionTime = record.FinishedAt.Sub(record.StartedAt)
	record.TotalElapsed = record.FinishedAt.Sub(record.DispatchedAt)
	if err != nil {
		record.Status = AttemptStatusCancelledAfterStart
		record.CancellationReason = cancellationReasonFromContext(ctx)
		return primaryAttemptOutcome{record: record}
	}

	record.Status = AttemptStatusCompleted
	return primaryAttemptOutcome{record: record, response: response}
}

// PrimaryOccupancy returns capacity observations for one primary instance.
func (o *BaselineOrchestrator) PrimaryOccupancy(serviceID ServiceID) (current, peak int, ok bool) {
	instance, ok := o.instances[serviceID]
	if !ok {
		return 0, 0, false
	}
	current, peak = instance.Occupancy()
	return current, peak, true
}

// Metrics returns a race-free snapshot of all terminal executions observed by
// this orchestrator.
func (o *BaselineOrchestrator) Metrics() MetricsSnapshot {
	return o.metrics.Snapshot()
}

func serviceOrder(serviceID ServiceID) int {
	for index, candidate := range serviceIDs {
		if serviceID == candidate {
			return index
		}
	}
	return len(serviceIDs)
}
