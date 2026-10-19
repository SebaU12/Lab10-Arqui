package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type AttemptType string

const (
	AttemptTypePrimary AttemptType = "primary"
	AttemptTypeRescue  AttemptType = "rescue"
)

type AttemptStatus string

const (
	AttemptStatusDispatched           AttemptStatus = "dispatched"
	AttemptStatusStarted              AttemptStatus = "started"
	AttemptStatusCompleted            AttemptStatus = "completed"
	AttemptStatusCancelledBeforeStart AttemptStatus = "cancelled_before_start"
	AttemptStatusCancelledAfterStart  AttemptStatus = "cancelled_after_start"
)

type CancellationReason string

const (
	CancellationReasonNone             CancellationReason = ""
	CancellationReasonHedgeLost        CancellationReason = "hedge_lost"
	CancellationReasonRequestCompleted CancellationReason = "request_completed"
	CancellationReasonRequestTimeout   CancellationReason = "request_timeout"
)

// Attempt describes one primary or rescue execution. Runtime timestamps and
// its terminal outcome are recorded separately in AttemptRecord.
type Attempt struct {
	ID             string
	RequestID      int
	ServiceID      ServiceID
	Type           AttemptType
	InstanceID     string
	PlannedLatency time.Duration
	PlannedSlow    bool
}

type AttemptRecord struct {
	Attempt            Attempt
	Status             AttemptStatus
	CancellationReason CancellationReason
	DispatchedAt       time.Time
	StartedAt          time.Time
	FinishedAt         time.Time
	QueueWait          time.Duration
	ExecutionTime      time.Duration
	TotalElapsed       time.Duration
	Winner             bool
}

// RescueRequestStats records hedging decisions that cannot be reconstructed
// solely from dispatched attempts.
type RescueRequestStats struct {
	Candidates        int
	Activated         int
	SkippedNoCapacity int
	Successful        int
}

var (
	errHedgeLost        = errors.New("redundant attempt lost hedge race")
	errRequestCompleted = errors.New("client request completed")
)

type rescueWorker struct {
	id            string
	slots         chan struct{}
	occupancy     int
	peakOccupancy int
}

// RescueWorkerSnapshot exposes capacity observations without exposing a
// worker's semaphore.
type RescueWorkerSnapshot struct {
	WorkerID      string
	Capacity      int
	Occupancy     int
	PeakOccupancy int
}

// RescuePoolSnapshot is a point-in-time, race-free view of the shared pool.
type RescuePoolSnapshot struct {
	Workers              []RescueWorkerSnapshot
	TotalCapacity        int
	Occupancy            int
	PeakOccupancy        int
	ReservationsAccepted int
	ReservationsRejected int
	UtilizationPercent   float64
}

// RescuePool owns a fixed set of shared workers. Its mutex makes reservation,
// release, occupancy accounting, and reset one coherent state transition.
type RescuePool struct {
	mu                sync.Mutex
	workers           []*rescueWorker
	capacityPerWorker int
	occupancy         int
	peakOccupancy     int
	accepted          int
	rejected          int
	startedAt         time.Time
	lastChange        time.Time
	occupiedNanos     float64
}

func NewRescuePool(workerCount, capacityPerWorker int) (*RescuePool, error) {
	if workerCount < 0 {
		return nil, fmt.Errorf("rescue worker count cannot be negative: %d", workerCount)
	}
	if capacityPerWorker <= 0 {
		return nil, fmt.Errorf("rescue worker capacity must be greater than zero: %d", capacityPerWorker)
	}

	now := time.Now()
	pool := &RescuePool{
		workers:           make([]*rescueWorker, 0, workerCount),
		capacityPerWorker: capacityPerWorker,
		startedAt:         now,
		lastChange:        now,
	}
	for index := 0; index < workerCount; index++ {
		pool.workers = append(pool.workers, &rescueWorker{
			id:    fmt.Sprintf("rescue-worker-%d", index+1),
			slots: make(chan struct{}, capacityPerWorker),
		})
	}
	return pool, nil
}

type rescueReservationState uint8

const (
	rescueReservationReserved rescueReservationState = iota
	rescueReservationExecuting
	rescueReservationReleased
)

// RescueReservation represents one capacity slot. Execute consumes it for one
// handler call; Release returns an unused reservation. Neither operation can
// release the same slot twice.
type RescueReservation struct {
	mu     sync.Mutex
	pool   *RescuePool
	worker *rescueWorker
	state  rescueReservationState
}

// TryReserve checks workers in stable numeric order and returns immediately.
// A nil result means that every worker was full; no attempt has started.
func (p *RescuePool) TryReserve() (*RescueReservation, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, worker := range p.workers {
		select {
		case worker.slots <- struct{}{}:
			now := time.Now()
			p.updateOccupiedTimeLocked(now)
			worker.occupancy++
			p.occupancy++
			if worker.occupancy > worker.peakOccupancy {
				worker.peakOccupancy = worker.occupancy
			}
			if p.occupancy > p.peakOccupancy {
				p.peakOccupancy = p.occupancy
			}
			p.accepted++
			return &RescueReservation{
				pool:   p,
				worker: worker,
				state:  rescueReservationReserved,
			}, true
		default:
		}
	}

	p.rejected++
	return nil, false
}

// WorkerID identifies the worker whose slot was reserved.
func (r *RescueReservation) WorkerID() string {
	if r == nil || r.worker == nil {
		return ""
	}
	return r.worker.id
}

// Execute runs any logical service using this reservation and always returns
// the capacity slot after success, handler error, or context cancellation.
func (r *RescueReservation) Execute(
	ctx context.Context,
	service LogicalService,
	requestID int,
	plannedLatency time.Duration,
) (ServiceResponse, error) {
	if r == nil {
		return ServiceResponse{}, fmt.Errorf("execute rescue: nil reservation")
	}

	r.mu.Lock()
	if r.state != rescueReservationReserved {
		r.mu.Unlock()
		return ServiceResponse{}, fmt.Errorf("execute rescue: reservation is not available")
	}
	r.state = rescueReservationExecuting
	r.mu.Unlock()

	defer r.finishExecution()
	return service.Handle(ctx, requestID, plannedLatency)
}

// Release returns a reservation that was not dispatched. It reports whether
// this call performed the release.
func (r *RescueReservation) Release() bool {
	if r == nil {
		return false
	}

	r.mu.Lock()
	if r.state != rescueReservationReserved {
		r.mu.Unlock()
		return false
	}
	r.state = rescueReservationReleased
	r.mu.Unlock()
	r.pool.release(r.worker)
	return true
}

func (r *RescueReservation) finishExecution() {
	r.mu.Lock()
	if r.state != rescueReservationExecuting {
		r.mu.Unlock()
		return
	}
	r.state = rescueReservationReleased
	r.mu.Unlock()
	r.pool.release(r.worker)
}

func (p *RescuePool) release(worker *rescueWorker) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	p.updateOccupiedTimeLocked(now)
	worker.occupancy--
	p.occupancy--
	<-worker.slots
}

// Snapshot reports both per-worker and pool-wide occupancy. Utilization is
// time-weighted over the pool lifetime and expressed on a 0-100 scale.
func (p *RescuePool) Snapshot() RescuePoolSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	occupiedNanos := p.occupiedNanos + float64(now.Sub(p.lastChange))*float64(p.occupancy)
	totalCapacity := len(p.workers) * p.capacityPerWorker
	utilization := 0.0
	if totalCapacity > 0 && now.After(p.startedAt) {
		utilization = occupiedNanos / (float64(now.Sub(p.startedAt)) * float64(totalCapacity)) * 100
	}

	snapshot := RescuePoolSnapshot{
		Workers:              make([]RescueWorkerSnapshot, 0, len(p.workers)),
		TotalCapacity:        totalCapacity,
		Occupancy:            p.occupancy,
		PeakOccupancy:        p.peakOccupancy,
		ReservationsAccepted: p.accepted,
		ReservationsRejected: p.rejected,
		UtilizationPercent:   utilization,
	}
	for _, worker := range p.workers {
		snapshot.Workers = append(snapshot.Workers, RescueWorkerSnapshot{
			WorkerID:      worker.id,
			Capacity:      p.capacityPerWorker,
			Occupancy:     worker.occupancy,
			PeakOccupancy: worker.peakOccupancy,
		})
	}
	return snapshot
}

// Reset clears all observations for a new experiment. Active reservations
// make reset unsafe, so the call fails until occupancy reaches zero.
func (p *RescuePool) Reset() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.occupancy != 0 {
		return fmt.Errorf("reset rescue pool: %d reservations are still active", p.occupancy)
	}

	for _, worker := range p.workers {
		if len(worker.slots) != 0 {
			return fmt.Errorf("reset rescue pool: worker %s still owns capacity", worker.id)
		}
		worker.occupancy = 0
		worker.peakOccupancy = 0
	}
	p.occupancy = 0
	p.peakOccupancy = 0
	p.accepted = 0
	p.rejected = 0
	p.occupiedNanos = 0
	now := time.Now()
	p.startedAt = now
	p.lastChange = now
	return nil
}

func (p *RescuePool) updateOccupiedTimeLocked(now time.Time) {
	p.occupiedNanos += float64(now.Sub(p.lastChange)) * float64(p.occupancy)
	p.lastChange = now
}

type proposalServiceState struct {
	service         LogicalService
	context         context.Context
	dispatchedAt    time.Time
	cancel          context.CancelCauseFunc
	resolved        bool
	hedgeConsidered bool
	tieKey          uint64
}

// ProposalOrchestrator owns a fresh set of primaries plus the shared rescue
// pool. It uses the same primary latency plan as the baseline.
type ProposalOrchestrator struct {
	config     Config
	plan       LatencyPlan
	instances  map[ServiceID]*PrimaryInstance
	rescuePool *RescuePool
	metrics    *MetricsCollector
}

func NewProposalOrchestrator(config Config, plan LatencyPlan) (*ProposalOrchestrator, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("create proposal orchestrator: %w", err)
	}
	if plan.RequestCount() != config.RequestCount {
		return nil, fmt.Errorf(
			"create proposal orchestrator: latency plan contains %d requests, want %d",
			plan.RequestCount(),
			config.RequestCount,
		)
	}

	instances := make(map[ServiceID]*PrimaryInstance, ServiceCount)
	for _, service := range Services() {
		for requestID := 1; requestID <= config.RequestCount; requestID++ {
			if _, ok := plan.Latencies(requestID, service.ID); !ok {
				return nil, fmt.Errorf(
					"create proposal orchestrator: missing latency for request %d and service %s",
					requestID,
					service.ID,
				)
			}
		}
		instances[service.ID] = newPrimaryInstance(service, config.CapacityPerInstance)
	}
	rescuePool, err := NewRescuePool(config.RescueWorkers, config.CapacityPerInstance)
	if err != nil {
		return nil, fmt.Errorf("create proposal orchestrator: %w", err)
	}

	return &ProposalOrchestrator{
		config:     config,
		plan:       plan,
		instances:  instances,
		rescuePool: rescuePool,
		metrics:    NewMetricsCollector(ArchitectureProposal),
	}, nil
}

// Execute runs the ten primaries and selectively dispatches at most the
// configured number of one-shot rescue attempts after the hedge threshold.
func (o *ProposalOrchestrator) Execute(
	ctx context.Context,
	request ClientRequest,
) (RequestExecution, error) {
	if request.ID < 1 || request.ID > o.plan.RequestCount() {
		return RequestExecution{}, fmt.Errorf(
			"execute proposal request: request ID %d outside latency plan range [1, %d]",
			request.ID,
			o.plan.RequestCount(),
		)
	}
	startedAt := time.Now()
	requestContext, cancelRequest := context.WithTimeout(ctx, o.config.Timeout)
	defer cancelRequest()

	latenciesByService := make(map[ServiceID]ServiceLatencies, ServiceCount)
	for _, serviceID := range serviceIDs {
		latencies, ok := o.plan.Latencies(request.ID, serviceID)
		if !ok {
			return RequestExecution{}, fmt.Errorf(
				"execute proposal request: missing latency for request %d and service %s",
				request.ID,
				serviceID,
			)
		}
		latenciesByService[serviceID] = latencies
	}

	maximumAttempts := ServiceCount * (1 + o.config.MaxRescueAttemptsPerService)
	outcomes := make(chan primaryAttemptOutcome, maximumAttempts)
	states := make(map[ServiceID]*proposalServiceState, ServiceCount)
	response := AggregatedResponse{
		RequestID: request.ID,
		Status:    RequestStatusReceived,
		Results:   make(map[ServiceID]ServiceResponse, ServiceCount),
		StartedAt: startedAt,
	}
	records := make([]AttemptRecord, 0, maximumAttempts)
	rescueStats := RescueRequestStats{}
	dispatchedAttempts := 0
	receivedAttempts := 0
	terminal := false

	var waitGroup sync.WaitGroup
	for index, service := range Services() {
		serviceContext, cancelService := context.WithCancelCause(requestContext)
		dispatchedAt := time.Now()
		state := &proposalServiceState{
			service:      service,
			context:      serviceContext,
			dispatchedAt: dispatchedAt,
			cancel:       cancelService,
			tieKey: rescueTieKey(
				o.config.Seed,
				request.ID,
				index,
			),
		}
		states[service.ID] = state

		primaryLatency := latenciesByService[service.ID].Primary
		record := AttemptRecord{
			Attempt: Attempt{
				ID:             fmt.Sprintf("proposal-r%d-%s-primary", request.ID, service.ID),
				RequestID:      request.ID,
				ServiceID:      service.ID,
				Type:           AttemptTypePrimary,
				InstanceID:     o.instances[service.ID].id,
				PlannedLatency: primaryLatency.Duration,
				PlannedSlow:    primaryLatency.Slow,
			},
			Status:       AttemptStatusDispatched,
			DispatchedAt: dispatchedAt,
		}
		instance := o.instances[service.ID]
		o.config.logEvent(
			ArchitectureProposal,
			request.ID,
			"PRIMARY_DISPATCHED",
			"service=%s instance=%s planned=%s slow=%t",
			service.ID,
			instance.id,
			primaryLatency.Duration,
			primaryLatency.Slow,
		)
		dispatchedAttempts++
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			outcomes <- instance.execute(serviceContext, record)
		}()
	}

	var hedgeTimer *time.Timer
	var hedgeEvents <-chan time.Time
	setNextHedgeTimer := func(now time.Time) {
		delay, ok := nextHedgeDelay(states, o.config, now)
		if !ok {
			hedgeEvents = nil
			return
		}
		if delay < 0 {
			delay = 0
		}
		if hedgeTimer == nil {
			hedgeTimer = time.NewTimer(delay)
		} else {
			hedgeTimer.Reset(delay)
		}
		hedgeEvents = hedgeTimer.C
	}
	setNextHedgeTimer(time.Now())
	defer func() {
		if hedgeTimer != nil {
			hedgeTimer.Stop()
		}
	}()

	processOutcome := func(outcome primaryAttemptOutcome) {
		receivedAttempts++
		state := states[outcome.record.Attempt.ServiceID]
		valid := outcome.record.Status == AttemptStatusCompleted &&
			outcome.response.RequestID == request.ID &&
			outcome.response.ServiceID == outcome.record.Attempt.ServiceID &&
			outcome.response.Status == ServiceResponseOK
		if !terminal && !state.resolved && valid {
			outcome.record.Winner = true
			state.resolved = true
			response.Results[outcome.response.ServiceID] = outcome.response
			state.cancel(errHedgeLost)
			if outcome.record.Attempt.Type == AttemptTypeRescue {
				rescueStats.Successful++
			}
			o.config.logEvent(
				ArchitectureProposal,
				request.ID,
				"RESULT_ACCEPTED",
				"service=%s type=%s instance=%s total=%s",
				outcome.record.Attempt.ServiceID,
				outcome.record.Attempt.Type,
				outcome.record.Attempt.InstanceID,
				outcome.record.TotalElapsed,
			)
		} else if outcome.record.Status == AttemptStatusCancelledBeforeStart ||
			outcome.record.Status == AttemptStatusCancelledAfterStart {
			o.config.logEvent(
				ArchitectureProposal,
				request.ID,
				"ATTEMPT_CANCELLED",
				"service=%s type=%s instance=%s status=%s reason=%s",
				outcome.record.Attempt.ServiceID,
				outcome.record.Attempt.Type,
				outcome.record.Attempt.InstanceID,
				outcome.record.Status,
				outcome.record.CancellationReason,
			)
		}
		records = append(records, outcome.record)

		if !terminal && len(response.Results) == ServiceCount {
			response.Status = RequestStatusCompleted
			response.FinishedAt = time.Now()
			response.Elapsed = response.FinishedAt.Sub(response.StartedAt)
			terminal = true
			cancelRequest()
			o.config.logEvent(
				ArchitectureProposal,
				request.ID,
				"REQUEST_COMPLETED",
				"elapsed=%s results=%d rescues_activated=%d rescues_successful=%d",
				response.Elapsed,
				len(response.Results),
				rescueStats.Activated,
				rescueStats.Successful,
			)
		}
	}

	for !terminal {
		select {
		case outcome := <-outcomes:
			processOutcome(outcome)
		case <-hedgeEvents:
			// Prefer results that were already published at the threshold over
			// starting a redundant attempt for an operation that has finished.
			draining := true
			for draining && !terminal {
				select {
				case outcome := <-outcomes:
					processOutcome(outcome)
				default:
					draining = false
				}
			}
			if terminal {
				continue
			}

			now := time.Now()
			candidates := eligibleRescueCandidates(states, o.config.HedgeThreshold, now)
			for _, state := range candidates {
				state.hedgeConsidered = true
				rescueStats.Candidates++
				o.config.logEvent(
					ArchitectureProposal,
					request.ID,
					"HEDGE_THRESHOLD_EXCEEDED",
					"service=%s pending=%s threshold=%s",
					state.service.ID,
					now.Sub(state.dispatchedAt),
					o.config.HedgeThreshold,
				)
				if rescueStats.Activated >= o.config.RescueBudgetPerRequest {
					o.config.logEvent(
						ArchitectureProposal,
						request.ID,
						"RESCUE_SKIPPED",
						"service=%s reason=budget_exhausted budget=%d",
						state.service.ID,
						o.config.RescueBudgetPerRequest,
					)
					continue
				}

				reservation, ok := o.rescuePool.TryReserve()
				if !ok {
					rescueStats.SkippedNoCapacity++
					o.config.logEvent(
						ArchitectureProposal,
						request.ID,
						"RESCUE_SKIPPED",
						"service=%s reason=no_capacity",
						state.service.ID,
					)
					continue
				}

				rescueLatency := latenciesByService[state.service.ID].Rescue
				dispatchedAt := time.Now()
				record := AttemptRecord{
					Attempt: Attempt{
						ID:             fmt.Sprintf("proposal-r%d-%s-rescue", request.ID, state.service.ID),
						RequestID:      request.ID,
						ServiceID:      state.service.ID,
						Type:           AttemptTypeRescue,
						InstanceID:     reservation.WorkerID(),
						PlannedLatency: rescueLatency.Duration,
						PlannedSlow:    rescueLatency.Slow,
					},
					Status:       AttemptStatusDispatched,
					DispatchedAt: dispatchedAt,
				}
				rescueStats.Activated++
				o.config.logEvent(
					ArchitectureProposal,
					request.ID,
					"RESCUE_ASSIGNED",
					"service=%s worker=%s planned=%s slow=%t",
					state.service.ID,
					reservation.WorkerID(),
					rescueLatency.Duration,
					rescueLatency.Slow,
				)
				dispatchedAttempts++
				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					outcomes <- executeRescueAttempt(
						state.service,
						state.context,
						reservation,
						record,
					)
				}()
			}
			setNextHedgeTimer(time.Now())
		case <-requestContext.Done():
			response.Status = RequestStatusTimedOut
			response.FinishedAt = time.Now()
			response.Elapsed = response.FinishedAt.Sub(response.StartedAt)
			terminal = true
			cancelRequest()
		}
	}

	for receivedAttempts < dispatchedAttempts {
		processOutcome(<-outcomes)
	}
	waitGroup.Wait()
	for _, state := range states {
		state.cancel(errRequestCompleted)
	}

	sort.Slice(records, func(left, right int) bool {
		leftService := serviceOrder(records[left].Attempt.ServiceID)
		rightService := serviceOrder(records[right].Attempt.ServiceID)
		if leftService != rightService {
			return leftService < rightService
		}
		return records[left].Attempt.Type == AttemptTypePrimary && records[right].Attempt.Type == AttemptTypeRescue
	})

	execution := RequestExecution{
		Response: response,
		Attempts: records,
		Rescues:  rescueStats,
	}
	o.metrics.Record(execution)
	return execution, nil
}

// Metrics returns a race-free snapshot of proposal request measurements.
func (o *ProposalOrchestrator) Metrics() MetricsSnapshot {
	return o.metrics.Snapshot()
}

func (o *ProposalOrchestrator) RescuePoolSnapshot() RescuePoolSnapshot {
	return o.rescuePool.Snapshot()
}

func (o *ProposalOrchestrator) PrimaryOccupancy(serviceID ServiceID) (current, peak int, ok bool) {
	instance, ok := o.instances[serviceID]
	if !ok {
		return 0, 0, false
	}
	current, peak = instance.Occupancy()
	return current, peak, true
}

func executeRescueAttempt(
	service LogicalService,
	ctx context.Context,
	reservation *RescueReservation,
	record AttemptRecord,
) primaryAttemptOutcome {
	record.Status = AttemptStatusStarted
	record.StartedAt = time.Now()
	record.QueueWait = record.StartedAt.Sub(record.DispatchedAt)
	response, err := reservation.Execute(
		ctx,
		service,
		record.Attempt.RequestID,
		record.Attempt.PlannedLatency,
	)
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

func nextHedgeDelay(
	states map[ServiceID]*proposalServiceState,
	config Config,
	now time.Time,
) (time.Duration, bool) {
	if config.MaxRescueAttemptsPerService == 0 {
		return 0, false
	}

	var earliest time.Time
	for _, state := range states {
		if state.resolved || state.hedgeConsidered {
			continue
		}
		eligibleAt := state.dispatchedAt.Add(config.HedgeThreshold)
		if earliest.IsZero() || eligibleAt.Before(earliest) {
			earliest = eligibleAt
		}
	}
	if earliest.IsZero() {
		return 0, false
	}
	return earliest.Sub(now), true
}

func eligibleRescueCandidates(
	states map[ServiceID]*proposalServiceState,
	threshold time.Duration,
	now time.Time,
) []*proposalServiceState {
	candidates := make([]*proposalServiceState, 0, ServiceCount)
	for _, state := range states {
		if state.resolved || state.hedgeConsidered || now.Sub(state.dispatchedAt) < threshold {
			continue
		}
		candidates = append(candidates, state)
	}
	sort.Slice(candidates, func(left, right int) bool {
		leftAge := now.Sub(candidates[left].dispatchedAt)
		rightAge := now.Sub(candidates[right].dispatchedAt)
		if leftAge != rightAge {
			return leftAge > rightAge
		}
		return candidates[left].tieKey < candidates[right].tieKey
	})
	return candidates
}

func rescueTieKey(seed int64, requestID, serviceIndex int) uint64 {
	stream := uint64(requestID)*ServiceCount + uint64(serviceIndex) + 2
	return uint64(deriveRandomSeed(seed, stream))
}

func cancellationReasonFromContext(ctx context.Context) CancellationReason {
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, errHedgeLost):
		return CancellationReasonHedgeLost
	case errors.Is(cause, errRequestCompleted):
		return CancellationReasonRequestCompleted
	case errors.Is(cause, context.DeadlineExceeded):
		return CancellationReasonRequestTimeout
	case errors.Is(cause, context.Canceled):
		return CancellationReasonRequestCompleted
	default:
		return CancellationReasonNone
	}
}
