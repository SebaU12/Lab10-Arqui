package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	config := DefaultConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() returned an error: %v", err)
	}

	if config.RequestCount != 100 {
		t.Errorf("RequestCount = %d, want 100", config.RequestCount)
	}
	if config.MaxClientConcurrency != 5 {
		t.Errorf("MaxClientConcurrency = %d, want 5", config.MaxClientConcurrency)
	}
	if config.SlowProbability != 0.03 {
		t.Errorf("SlowProbability = %g, want 0.03", config.SlowProbability)
	}
	if config.NormalLatencyMin != 10*time.Millisecond || config.NormalLatencyMax != 30*time.Millisecond {
		t.Errorf("normal latency range = [%s, %s], want [10ms, 30ms]", config.NormalLatencyMin, config.NormalLatencyMax)
	}
	if config.SlowLatencyMin != 400*time.Millisecond || config.SlowLatencyMax != 1500*time.Millisecond {
		t.Errorf("slow latency range = [%s, %s], want [400ms, 1.5s]", config.SlowLatencyMin, config.SlowLatencyMax)
	}
	if config.HedgeThreshold != 150*time.Millisecond {
		t.Errorf("HedgeThreshold = %s, want 150ms", config.HedgeThreshold)
	}
	if config.RescueWorkers != 2 {
		t.Errorf("RescueWorkers = %d, want 2", config.RescueWorkers)
	}
	if config.CapacityPerInstance != 5 {
		t.Errorf("CapacityPerInstance = %d, want 5", config.CapacityPerInstance)
	}
	if config.RescueBudgetPerRequest != 3 {
		t.Errorf("RescueBudgetPerRequest = %d, want 3", config.RescueBudgetPerRequest)
	}
	if config.MaxRescueAttemptsPerService != 1 {
		t.Errorf("MaxRescueAttemptsPerService = %d, want 1", config.MaxRescueAttemptsPerService)
	}
	if config.Timeout != 5*time.Second {
		t.Errorf("Timeout = %s, want 5s", config.Timeout)
	}
	if config.Seed != 2026 {
		t.Errorf("Seed = %d, want 2026", config.Seed)
	}
	if config.Verbose || config.LogRequestID != 0 {
		t.Errorf("logging defaults = verbose:%t request:%d, want false/0", config.Verbose, config.LogRequestID)
	}
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mutate    func(*Config)
		wantError string
	}{
		{name: "zero requests", mutate: func(c *Config) { c.RequestCount = 0 }, wantError: "request count"},
		{name: "zero concurrency", mutate: func(c *Config) { c.MaxClientConcurrency = 0 }, wantError: "max client concurrency"},
		{name: "negative probability", mutate: func(c *Config) { c.SlowProbability = -0.1 }, wantError: "slow probability"},
		{name: "probability over one", mutate: func(c *Config) { c.SlowProbability = 1.1 }, wantError: "slow probability"},
		{name: "not-a-number probability", mutate: func(c *Config) { c.SlowProbability = math.NaN() }, wantError: "slow probability"},
		{name: "negative normal minimum", mutate: func(c *Config) { c.NormalLatencyMin = -time.Millisecond }, wantError: "normal minimum latency"},
		{name: "inverted normal range", mutate: func(c *Config) { c.NormalLatencyMax = c.NormalLatencyMin - time.Nanosecond }, wantError: "normal maximum latency"},
		{name: "negative slow minimum", mutate: func(c *Config) { c.SlowLatencyMin = -time.Millisecond }, wantError: "slow minimum latency"},
		{name: "inverted slow range", mutate: func(c *Config) { c.SlowLatencyMax = c.SlowLatencyMin - time.Nanosecond }, wantError: "slow maximum latency"},
		{name: "zero hedge threshold", mutate: func(c *Config) { c.HedgeThreshold = 0 }, wantError: "hedge threshold"},
		{name: "negative workers", mutate: func(c *Config) { c.RescueWorkers = -1 }, wantError: "rescue workers"},
		{name: "zero capacity", mutate: func(c *Config) { c.CapacityPerInstance = 0 }, wantError: "capacity per instance"},
		{name: "negative budget", mutate: func(c *Config) { c.RescueBudgetPerRequest = -1 }, wantError: "rescue budget"},
		{name: "negative per-service limit", mutate: func(c *Config) { c.MaxRescueAttemptsPerService = -1 }, wantError: "max rescue attempts"},
		{name: "per-service limit over one", mutate: func(c *Config) { c.MaxRescueAttemptsPerService = 2 }, wantError: "max rescue attempts"},
		{name: "zero timeout", mutate: func(c *Config) { c.Timeout = 0 }, wantError: "timeout"},
		{name: "negative log request", mutate: func(c *Config) { c.LogRequestID = -1 }, wantError: "log request ID"},
		{name: "log request outside plan", mutate: func(c *Config) { c.LogRequestID = c.RequestCount + 1 }, wantError: "log request ID"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			config := DefaultConfig()
			test.mutate(&config)
			err := config.Validate()
			if err == nil {
				t.Fatal("Validate() returned nil, want an error")
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Errorf("Validate() error = %q, want it to contain %q", err, test.wantError)
			}
		})
	}
}

func TestConfigAllowsHedgingToBeDisabled(t *testing.T) {
	t.Parallel()

	config := DefaultConfig()
	config.RescueWorkers = 0
	config.RescueBudgetPerRequest = 0
	config.MaxRescueAttemptsPerService = 0

	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() rejected disabled hedging: %v", err)
	}
}

func TestServiceIDsAreCompleteUniqueAndStable(t *testing.T) {
	t.Parallel()

	want := []ServiceID{
		UserService,
		ProductService,
		InventoryService,
		PricingService,
		PromotionService,
		TaxService,
		ShippingService,
		PaymentService,
		RiskService,
		RecommendationService,
	}

	got := ServiceIDs()
	if len(got) != ServiceCount {
		t.Fatalf("len(ServiceIDs()) = %d, want %d", len(got), ServiceCount)
	}

	seen := make(map[ServiceID]bool, len(got))
	for index, id := range got {
		if id != want[index] {
			t.Errorf("ServiceIDs()[%d] = %q, want %q", index, id, want[index])
		}
		if seen[id] {
			t.Errorf("duplicate service ID %q", id)
		}
		seen[id] = true
	}

	got[0] = ServiceID("changed")
	if ServiceIDs()[0] != UserService {
		t.Error("ServiceIDs returned mutable canonical storage")
	}
}

func TestLogicalServicesReturnDeterministicEquivalentResponses(t *testing.T) {
	t.Parallel()

	services := Services()
	if len(services) != ServiceCount {
		t.Fatalf("len(Services()) = %d, want %d", len(services), ServiceCount)
	}

	for index, service := range services {
		if service.ID != ServiceIDs()[index] {
			t.Errorf("Services()[%d].ID = %q, want %q", index, service.ID, ServiceIDs()[index])
		}

		primaryResponse, err := service.Handle(context.Background(), 42, 0)
		if err != nil {
			t.Fatalf("primary %s Handle() returned an error: %v", service.ID, err)
		}
		rescueResponse, err := service.Handle(context.Background(), 42, 0)
		if err != nil {
			t.Fatalf("rescue %s Handle() returned an error: %v", service.ID, err)
		}

		if primaryResponse != rescueResponse {
			t.Errorf("%s primary response = %#v, rescue response = %#v", service.ID, primaryResponse, rescueResponse)
		}
		if primaryResponse.RequestID != 42 {
			t.Errorf("%s response RequestID = %d, want 42", service.ID, primaryResponse.RequestID)
		}
		if primaryResponse.ServiceID != service.ID {
			t.Errorf("%s response ServiceID = %q", service.ID, primaryResponse.ServiceID)
		}
		if primaryResponse.Status != ServiceResponseOK {
			t.Errorf("%s response Status = %q, want %q", service.ID, primaryResponse.Status, ServiceResponseOK)
		}
		if primaryResponse.Data == "" {
			t.Errorf("%s response Data is empty", service.ID)
		}
	}
}

func TestLogicalServiceWaitIsCancelable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	_, err := (LogicalService{ID: UserService}).Handle(ctx, 1, 2*time.Second)
	elapsed := time.Since(startedAt)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Handle() error = %v, want context deadline exceeded", err)
	}
	if elapsed >= 500*time.Millisecond {
		t.Errorf("cancelled Handle() took %s, want less than 500ms", elapsed)
	}
}

func TestLogicalServiceRejectsNegativeLatency(t *testing.T) {
	t.Parallel()

	_, err := (LogicalService{ID: UserService}).Handle(context.Background(), 1, -time.Nanosecond)
	if err == nil {
		t.Fatal("Handle() returned nil error for negative latency")
	}
}

func TestLatencyPlanIsCompleteReproducibleAndInRange(t *testing.T) {
	t.Parallel()

	config := DefaultConfig()
	first := generatePlanForTest(t, config)
	second := generatePlanForTest(t, config)

	if first.RequestCount() != config.RequestCount {
		t.Fatalf("RequestCount() = %d, want %d", first.RequestCount(), config.RequestCount)
	}

	primaryDiffersFromRescue := false
	for requestID := 1; requestID <= config.RequestCount; requestID++ {
		for _, serviceID := range ServiceIDs() {
			firstLatencies, ok := first.Latencies(requestID, serviceID)
			if !ok {
				t.Fatalf("missing first plan entry for request %d and service %s", requestID, serviceID)
			}
			secondLatencies, ok := second.Latencies(requestID, serviceID)
			if !ok {
				t.Fatalf("missing second plan entry for request %d and service %s", requestID, serviceID)
			}
			if firstLatencies != secondLatencies {
				t.Errorf("non-reproducible entry for request %d and service %s: %#v != %#v", requestID, serviceID, firstLatencies, secondLatencies)
			}
			assertLatencySampleInConfiguredRange(t, firstLatencies.Primary, config)
			assertLatencySampleInConfiguredRange(t, firstLatencies.Rescue, config)
			if firstLatencies.Primary != firstLatencies.Rescue {
				primaryDiffersFromRescue = true
			}
		}
	}

	if !primaryDiffersFromRescue {
		t.Error("all primary samples equal their rescue samples; want independently generated streams")
	}
	if _, ok := first.Latencies(0, UserService); ok {
		t.Error("plan unexpectedly contains request ID 0")
	}
	if _, ok := first.Latencies(config.RequestCount+1, UserService); ok {
		t.Error("plan unexpectedly contains a request beyond RequestCount")
	}
	if _, ok := first.Latencies(1, ServiceID("UnknownService")); ok {
		t.Error("plan unexpectedly contains an unknown service")
	}
}

func TestLatencyPlanChangesWithSeed(t *testing.T) {
	t.Parallel()

	firstConfig := DefaultConfig()
	secondConfig := firstConfig
	secondConfig.Seed++

	first := generatePlanForTest(t, firstConfig)
	second := generatePlanForTest(t, secondConfig)
	if latencyPlansEqual(first, second) {
		t.Error("plans generated with different seeds are identical")
	}
}

func TestEveryServiceCanBeSlowAndSeveralCanBeSlowTogether(t *testing.T) {
	t.Parallel()

	config := DefaultConfig()
	config.RequestCount = 5000
	plan := generatePlanForTest(t, config)

	sawSlow := make(map[ServiceID]bool, ServiceCount)
	sawNormal := make(map[ServiceID]bool, ServiceCount)
	sawSeveralSlowTogether := false
	for requestID := 1; requestID <= config.RequestCount; requestID++ {
		slowForRequest := 0
		for _, serviceID := range ServiceIDs() {
			latencies, ok := plan.Latencies(requestID, serviceID)
			if !ok {
				t.Fatalf("missing plan entry for request %d and service %s", requestID, serviceID)
			}
			if latencies.Primary.Slow {
				sawSlow[serviceID] = true
				slowForRequest++
			} else {
				sawNormal[serviceID] = true
			}
		}
		if slowForRequest > 1 {
			sawSeveralSlowTogether = true
		}
	}

	for _, serviceID := range ServiceIDs() {
		if !sawSlow[serviceID] {
			t.Errorf("%s never received a slow primary sample", serviceID)
		}
		if !sawNormal[serviceID] {
			t.Errorf("%s was permanently slow", serviceID)
		}
	}
	if !sawSeveralSlowTogether {
		t.Error("no request had several slow services simultaneously")
	}
}

func TestLatencyPlanGenerationDoesNotShareConcurrentRandomState(t *testing.T) {
	t.Parallel()

	const generators = 4
	config := DefaultConfig()
	plans := make([]LatencyPlan, generators)
	errorsByGenerator := make([]error, generators)

	var waitGroup sync.WaitGroup
	waitGroup.Add(generators)
	for index := range plans {
		go func() {
			defer waitGroup.Done()
			plans[index], errorsByGenerator[index] = GenerateLatencyPlan(config)
		}()
	}
	waitGroup.Wait()

	for index, err := range errorsByGenerator {
		if err != nil {
			t.Fatalf("generator %d returned an error: %v", index, err)
		}
		if !latencyPlansEqual(plans[0], plans[index]) {
			t.Errorf("generator %d produced a different plan", index)
		}
	}
}

func TestGenerateLatencyPlanRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	config := DefaultConfig()
	config.RequestCount = 0
	if _, err := GenerateLatencyPlan(config); err == nil {
		t.Fatal("GenerateLatencyPlan() returned nil error for invalid config")
	}
}

func generatePlanForTest(t *testing.T, config Config) LatencyPlan {
	t.Helper()

	plan, err := GenerateLatencyPlan(config)
	if err != nil {
		t.Fatalf("GenerateLatencyPlan() returned an error: %v", err)
	}
	return plan
}

func latencyPlansEqual(first, second LatencyPlan) bool {
	if first.RequestCount() != second.RequestCount() {
		return false
	}
	for requestID := 1; requestID <= first.RequestCount(); requestID++ {
		for _, serviceID := range ServiceIDs() {
			firstLatencies, firstOK := first.Latencies(requestID, serviceID)
			secondLatencies, secondOK := second.Latencies(requestID, serviceID)
			if firstOK != secondOK || firstLatencies != secondLatencies {
				return false
			}
		}
	}
	return true
}

func assertLatencySampleInConfiguredRange(t *testing.T, sample LatencySample, config Config) {
	t.Helper()

	minimum := config.NormalLatencyMin
	maximum := config.NormalLatencyMax
	if sample.Slow {
		minimum = config.SlowLatencyMin
		maximum = config.SlowLatencyMax
	}
	if sample.Duration < minimum || sample.Duration > maximum {
		t.Errorf("sample duration %s outside configured range [%s, %s] (slow=%t)", sample.Duration, minimum, maximum, sample.Slow)
	}
}

func TestBaselineReturnsAllTenResultsConcurrently(t *testing.T) {
	t.Parallel()

	config := fixedLatencyConfig(1, 100*time.Millisecond)
	orchestrator := newBaselineForTest(t, config)

	execution, err := orchestrator.Execute(context.Background(), ClientRequest{ID: 1})
	if err != nil {
		t.Fatalf("Execute() returned an error: %v", err)
	}
	if execution.Response.Status != RequestStatusCompleted {
		t.Fatalf("response status = %q, want %q", execution.Response.Status, RequestStatusCompleted)
	}
	if len(execution.Response.Results) != ServiceCount {
		t.Fatalf("result count = %d, want %d", len(execution.Response.Results), ServiceCount)
	}
	if len(execution.Attempts) != ServiceCount {
		t.Fatalf("attempt count = %d, want %d", len(execution.Attempts), ServiceCount)
	}
	if execution.Response.Elapsed >= 500*time.Millisecond {
		t.Errorf("baseline elapsed = %s; ten 100ms services do not appear concurrent", execution.Response.Elapsed)
	}

	latestStart := execution.Attempts[0].StartedAt
	earliestFinish := execution.Attempts[0].FinishedAt
	for index, record := range execution.Attempts {
		wantServiceID := ServiceIDs()[index]
		if record.Attempt.ServiceID != wantServiceID {
			t.Errorf("attempt %d service = %q, want %q", index, record.Attempt.ServiceID, wantServiceID)
		}
		if record.Attempt.Type != AttemptTypePrimary {
			t.Errorf("%s attempt type = %q, want primary", record.Attempt.ServiceID, record.Attempt.Type)
		}
		if record.Status != AttemptStatusCompleted {
			t.Errorf("%s attempt status = %q, want completed", record.Attempt.ServiceID, record.Status)
		}
		if !record.Winner {
			t.Errorf("%s completed primary was not marked as winner", record.Attempt.ServiceID)
		}
		if record.CancellationReason != CancellationReasonNone {
			t.Errorf("%s cancellation reason = %q, want empty", record.Attempt.ServiceID, record.CancellationReason)
		}
		if record.StartedAt.IsZero() || record.FinishedAt.IsZero() {
			t.Errorf("%s attempt is missing runtime timestamps", record.Attempt.ServiceID)
		}
		if record.StartedAt.After(latestStart) {
			latestStart = record.StartedAt
		}
		if record.FinishedAt.Before(earliestFinish) {
			earliestFinish = record.FinishedAt
		}

		response, ok := execution.Response.Results[record.Attempt.ServiceID]
		if !ok {
			t.Errorf("aggregate is missing %s", record.Attempt.ServiceID)
			continue
		}
		if response.RequestID != 1 || response.ServiceID != record.Attempt.ServiceID {
			t.Errorf("invalid aggregate response for %s: %#v", record.Attempt.ServiceID, response)
		}
	}
	if !latestStart.Before(earliestFinish) {
		t.Errorf("attempt intervals did not all overlap: latest start %s, earliest finish %s", latestStart, earliestFinish)
	}
}

func TestBaselineNeverExceedsPrimaryCapacity(t *testing.T) {
	t.Parallel()

	const requests = 8
	config := fixedLatencyConfig(requests, 30*time.Millisecond)
	config.CapacityPerInstance = 2
	config.MaxClientConcurrency = requests
	orchestrator := newBaselineForTest(t, config)

	start := make(chan struct{})
	errorsByRequest := make(chan error, requests)
	var waitGroup sync.WaitGroup
	waitGroup.Add(requests)
	for requestID := 1; requestID <= requests; requestID++ {
		go func() {
			defer waitGroup.Done()
			<-start
			execution, err := orchestrator.Execute(context.Background(), ClientRequest{ID: requestID})
			if err == nil && execution.Response.Status != RequestStatusCompleted {
				err = fmt.Errorf("request %d status = %s", requestID, execution.Response.Status)
			}
			errorsByRequest <- err
		}()
	}
	close(start)
	waitGroup.Wait()
	close(errorsByRequest)

	for err := range errorsByRequest {
		if err != nil {
			t.Errorf("concurrent Execute() failed: %v", err)
		}
	}
	for _, serviceID := range ServiceIDs() {
		current, peak, ok := orchestrator.PrimaryOccupancy(serviceID)
		if !ok {
			t.Errorf("missing primary instance for %s", serviceID)
			continue
		}
		if current != 0 {
			t.Errorf("%s current occupancy = %d after completion, want 0", serviceID, current)
		}
		if peak != config.CapacityPerInstance {
			t.Errorf("%s peak occupancy = %d, want %d", serviceID, peak, config.CapacityPerInstance)
		}
	}
}

func TestBaselineTimeoutCancelsStartedAttemptsAndReleasesCapacity(t *testing.T) {
	t.Parallel()

	config := fixedLatencyConfig(1, time.Second)
	config.Timeout = 30 * time.Millisecond
	orchestrator := newBaselineForTest(t, config)

	execution, err := orchestrator.Execute(context.Background(), ClientRequest{ID: 1})
	if err != nil {
		t.Fatalf("Execute() returned an error: %v", err)
	}
	if execution.Response.Status != RequestStatusTimedOut {
		t.Fatalf("response status = %q, want %q", execution.Response.Status, RequestStatusTimedOut)
	}
	if execution.Response.Elapsed >= 500*time.Millisecond {
		t.Errorf("timed-out request took %s, want less than 500ms", execution.Response.Elapsed)
	}
	if len(execution.Attempts) != ServiceCount {
		t.Fatalf("attempt count = %d, want %d", len(execution.Attempts), ServiceCount)
	}

	for _, record := range execution.Attempts {
		if record.Status != AttemptStatusCancelledAfterStart {
			t.Errorf("%s status = %q, want cancelled_after_start", record.Attempt.ServiceID, record.Status)
		}
		if record.CancellationReason != CancellationReasonRequestTimeout {
			t.Errorf("%s cancellation reason = %q, want request_timeout", record.Attempt.ServiceID, record.CancellationReason)
		}
		if record.Winner {
			t.Errorf("%s cancelled attempt marked as winner", record.Attempt.ServiceID)
		}
		current, _, ok := orchestrator.PrimaryOccupancy(record.Attempt.ServiceID)
		if !ok || current != 0 {
			t.Errorf("%s occupancy after timeout = %d (exists=%t), want 0", record.Attempt.ServiceID, current, ok)
		}
	}
}

func TestBaselineRecordsCancellationBeforeAndAfterStart(t *testing.T) {
	t.Parallel()

	config := fixedLatencyConfig(2, time.Second)
	config.CapacityPerInstance = 1
	config.MaxClientConcurrency = 2
	orchestrator := newBaselineForTest(t, config)
	requestContext, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()

	start := make(chan struct{})
	executions := make(chan RequestExecution, 2)
	errorsByRequest := make(chan error, 2)
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	for requestID := 1; requestID <= 2; requestID++ {
		go func() {
			defer waitGroup.Done()
			<-start
			execution, err := orchestrator.Execute(requestContext, ClientRequest{ID: requestID})
			executions <- execution
			errorsByRequest <- err
		}()
	}
	close(start)
	time.Sleep(50 * time.Millisecond)
	cancelRequests()
	waitGroup.Wait()
	close(executions)
	close(errorsByRequest)

	for err := range errorsByRequest {
		if err != nil {
			t.Errorf("Execute() returned an error: %v", err)
		}
	}

	beforeStart := 0
	afterStart := 0
	for execution := range executions {
		if execution.Response.Status != RequestStatusTimedOut {
			t.Errorf("response status = %q, want timed_out", execution.Response.Status)
		}
		for _, record := range execution.Attempts {
			switch record.Status {
			case AttemptStatusCancelledBeforeStart:
				beforeStart++
				if !record.StartedAt.IsZero() {
					t.Errorf("%s cancelled-before-start attempt has StartedAt", record.Attempt.ID)
				}
			case AttemptStatusCancelledAfterStart:
				afterStart++
				if record.StartedAt.IsZero() {
					t.Errorf("%s cancelled-after-start attempt has no StartedAt", record.Attempt.ID)
				}
			default:
				t.Errorf("%s status = %q, want a cancellation", record.Attempt.ID, record.Status)
			}
		}
	}
	if beforeStart != ServiceCount {
		t.Errorf("cancelled before start = %d, want %d", beforeStart, ServiceCount)
	}
	if afterStart != ServiceCount {
		t.Errorf("cancelled after start = %d, want %d", afterStart, ServiceCount)
	}
	for _, serviceID := range ServiceIDs() {
		current, peak, _ := orchestrator.PrimaryOccupancy(serviceID)
		if current != 0 {
			t.Errorf("%s current occupancy = %d, want 0", serviceID, current)
		}
		if peak > config.CapacityPerInstance {
			t.Errorf("%s peak occupancy = %d, capacity = %d", serviceID, peak, config.CapacityPerInstance)
		}
	}
}

func TestBaselineRejectsMismatchedPlanAndUnknownRequest(t *testing.T) {
	t.Parallel()

	planConfig := fixedLatencyConfig(1, 0)
	plan := generatePlanForTest(t, planConfig)
	mismatchedConfig := planConfig
	mismatchedConfig.RequestCount = 2
	if _, err := NewBaselineOrchestrator(mismatchedConfig, plan); err == nil {
		t.Fatal("NewBaselineOrchestrator() accepted a mismatched latency plan")
	}

	orchestrator, err := NewBaselineOrchestrator(planConfig, plan)
	if err != nil {
		t.Fatalf("NewBaselineOrchestrator() returned an error: %v", err)
	}
	if _, err := orchestrator.Execute(context.Background(), ClientRequest{ID: 0}); err == nil {
		t.Fatal("Execute() accepted request ID 0")
	}
	if _, err := orchestrator.Execute(context.Background(), ClientRequest{ID: 2}); err == nil {
		t.Fatal("Execute() accepted a request beyond the plan")
	}
}

func fixedLatencyConfig(requestCount int, latency time.Duration) Config {
	config := DefaultConfig()
	config.RequestCount = requestCount
	config.MaxClientConcurrency = requestCount
	config.SlowProbability = 0
	config.NormalLatencyMin = latency
	config.NormalLatencyMax = latency
	config.Timeout = 5 * time.Second
	return config
}

func newBaselineForTest(t *testing.T, config Config) *BaselineOrchestrator {
	t.Helper()

	plan := generatePlanForTest(t, config)
	orchestrator, err := NewBaselineOrchestrator(config, plan)
	if err != nil {
		t.Fatalf("NewBaselineOrchestrator() returned an error: %v", err)
	}
	return orchestrator
}

func TestNearestRankPercentiles(t *testing.T) {
	t.Parallel()

	samples := []time.Duration{
		40 * time.Millisecond,
		10 * time.Millisecond,
		30 * time.Millisecond,
		20 * time.Millisecond,
	}
	original := append([]time.Duration(nil), samples...)
	tests := []struct {
		name       string
		percentile float64
		want       time.Duration
	}{
		{name: "minimum", percentile: 0.01, want: 10 * time.Millisecond},
		{name: "p50", percentile: 0.50, want: 20 * time.Millisecond},
		{name: "p75", percentile: 0.75, want: 30 * time.Millisecond},
		{name: "p95", percentile: 0.95, want: 40 * time.Millisecond},
		{name: "p99", percentile: 0.99, want: 40 * time.Millisecond},
		{name: "maximum", percentile: 1, want: 40 * time.Millisecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, ok := NearestRank(samples, test.percentile)
			if !ok {
				t.Fatalf("NearestRank(%g) returned no value", test.percentile)
			}
			if got != test.want {
				t.Errorf("NearestRank(%g) = %s, want %s", test.percentile, got, test.want)
			}
		})
	}

	for index := range samples {
		if samples[index] != original[index] {
			t.Fatalf("NearestRank changed input samples: got %v, want %v", samples, original)
		}
	}
}

func TestNearestRankRejectsEmptyOrInvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		samples    []time.Duration
		percentile float64
	}{
		{name: "empty", samples: nil, percentile: 0.5},
		{name: "zero", samples: []time.Duration{time.Second}, percentile: 0},
		{name: "negative", samples: []time.Duration{time.Second}, percentile: -0.1},
		{name: "over one", samples: []time.Duration{time.Second}, percentile: 1.1},
		{name: "not a number", samples: []time.Duration{time.Second}, percentile: math.NaN()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, ok := NearestRank(test.samples, test.percentile); ok {
				t.Errorf("NearestRank(%g) returned a value for invalid input", test.percentile)
			}
		})
	}
}

func TestMetricsCollectorCountsAttemptsAndIncludesTimeoutsInPercentiles(t *testing.T) {
	t.Parallel()

	collector := NewMetricsCollector(ArchitectureBaseline)
	collector.Record(RequestExecution{
		Response: AggregatedResponse{
			Status:  RequestStatusCompleted,
			Elapsed: 10 * time.Millisecond,
		},
		Attempts: []AttemptRecord{
			{Status: AttemptStatusCompleted, TotalElapsed: 900 * time.Millisecond},
			{Status: AttemptStatusCompleted, TotalElapsed: 800 * time.Millisecond},
		},
	})
	collector.Record(RequestExecution{
		Response: AggregatedResponse{
			Status:  RequestStatusTimedOut,
			Elapsed: 100 * time.Millisecond,
		},
		Attempts: []AttemptRecord{
			{Status: AttemptStatusCompleted, TotalElapsed: time.Millisecond},
			{Status: AttemptStatusCancelledBeforeStart},
			{Status: AttemptStatusCancelledAfterStart},
		},
	})

	snapshot := collector.Snapshot()
	if snapshot.Architecture != ArchitectureBaseline {
		t.Errorf("architecture = %q, want baseline", snapshot.Architecture)
	}
	if snapshot.ClientRequestsReceived != 2 {
		t.Errorf("received requests = %d, want 2", snapshot.ClientRequestsReceived)
	}
	if snapshot.ClientRequestsCompleted != 1 {
		t.Errorf("completed requests = %d, want 1", snapshot.ClientRequestsCompleted)
	}
	if snapshot.ClientRequestsTimedOut != 1 {
		t.Errorf("timed-out requests = %d, want 1", snapshot.ClientRequestsTimedOut)
	}
	if snapshot.AttemptsDispatched != 5 {
		t.Errorf("dispatched attempts = %d, want 5", snapshot.AttemptsDispatched)
	}
	if snapshot.AttemptsStarted != 4 {
		t.Errorf("started attempts = %d, want 4", snapshot.AttemptsStarted)
	}
	if snapshot.AttemptsCompleted != 3 {
		t.Errorf("completed attempts = %d, want 3", snapshot.AttemptsCompleted)
	}
	if snapshot.AttemptsCancelledBeforeStart != 1 {
		t.Errorf("cancelled-before-start attempts = %d, want 1", snapshot.AttemptsCancelledBeforeStart)
	}
	if snapshot.AttemptsCancelledAfterStart != 1 {
		t.Errorf("cancelled-after-start attempts = %d, want 1", snapshot.AttemptsCancelledAfterStart)
	}
	if !snapshot.HasPercentiles {
		t.Fatal("snapshot has no percentiles")
	}
	if snapshot.P50 != 10*time.Millisecond {
		t.Errorf("p50 = %s, want 10ms", snapshot.P50)
	}
	if snapshot.P95 != 100*time.Millisecond || snapshot.P99 != 100*time.Millisecond {
		t.Errorf("p95/p99 = %s/%s, want 100ms/100ms", snapshot.P95, snapshot.P99)
	}
	if len(snapshot.Latencies) != 2 || snapshot.Latencies[0] != 10*time.Millisecond || snapshot.Latencies[1] != 100*time.Millisecond {
		t.Errorf("latencies = %v, want [10ms 100ms]", snapshot.Latencies)
	}

	if snapshot.AttemptsDispatched != snapshot.AttemptsCompleted+snapshot.AttemptsCancelledBeforeStart+snapshot.AttemptsCancelledAfterStart {
		t.Error("dispatched attempt identity does not hold")
	}
	if snapshot.AttemptsStarted != snapshot.AttemptsCompleted+snapshot.AttemptsCancelledAfterStart {
		t.Error("started attempt identity does not hold")
	}

	snapshot.Latencies[0] = time.Hour
	if collector.Snapshot().Latencies[0] != 10*time.Millisecond {
		t.Error("mutating a snapshot changed collector storage")
	}
}

func TestMetricsCollectorSupportsConcurrentRecords(t *testing.T) {
	t.Parallel()

	const requestCount = 100
	collector := NewMetricsCollector(ArchitectureProposal)
	var waitGroup sync.WaitGroup
	waitGroup.Add(requestCount)
	for requestID := 1; requestID <= requestCount; requestID++ {
		go func() {
			defer waitGroup.Done()
			collector.Record(RequestExecution{
				Response: AggregatedResponse{
					RequestID: requestID,
					Status:    RequestStatusCompleted,
					Elapsed:   time.Duration(requestID) * time.Millisecond,
				},
				Attempts: []AttemptRecord{{Status: AttemptStatusCompleted}},
			})
		}()
	}
	waitGroup.Wait()

	snapshot := collector.Snapshot()
	if snapshot.ClientRequestsReceived != requestCount || snapshot.ClientRequestsCompleted != requestCount {
		t.Errorf("request counts = received %d, completed %d; want %d each", snapshot.ClientRequestsReceived, snapshot.ClientRequestsCompleted, requestCount)
	}
	if snapshot.AttemptsDispatched != requestCount || snapshot.AttemptsStarted != requestCount || snapshot.AttemptsCompleted != requestCount {
		t.Errorf("attempt counts = dispatched %d, started %d, completed %d; want %d each", snapshot.AttemptsDispatched, snapshot.AttemptsStarted, snapshot.AttemptsCompleted, requestCount)
	}
	if snapshot.P50 != 50*time.Millisecond {
		t.Errorf("p50 = %s, want 50ms", snapshot.P50)
	}
	if snapshot.P95 != 95*time.Millisecond {
		t.Errorf("p95 = %s, want 95ms", snapshot.P95)
	}
	if snapshot.P99 != 99*time.Millisecond {
		t.Errorf("p99 = %s, want 99ms", snapshot.P99)
	}
}

func TestBaselineAutomaticallyRecordsClientMetrics(t *testing.T) {
	t.Parallel()

	config := fixedLatencyConfig(2, 5*time.Millisecond)
	orchestrator := newBaselineForTest(t, config)
	for requestID := 1; requestID <= config.RequestCount; requestID++ {
		execution, err := orchestrator.Execute(context.Background(), ClientRequest{ID: requestID})
		if err != nil {
			t.Fatalf("request %d Execute() returned an error: %v", requestID, err)
		}
		if execution.Response.Elapsed != execution.Response.FinishedAt.Sub(execution.Response.StartedAt) {
			t.Errorf("request %d elapsed does not match monotonic timestamp subtraction", requestID)
		}
	}

	snapshot := orchestrator.Metrics()
	if snapshot.ClientRequestsReceived != 2 || snapshot.ClientRequestsCompleted != 2 || snapshot.ClientRequestsTimedOut != 0 {
		t.Errorf("client metrics = received %d, completed %d, timed out %d; want 2, 2, 0", snapshot.ClientRequestsReceived, snapshot.ClientRequestsCompleted, snapshot.ClientRequestsTimedOut)
	}
	if len(snapshot.Latencies) != 2 {
		t.Errorf("stored client latencies = %d, want 2", len(snapshot.Latencies))
	}
	if snapshot.AttemptsDispatched != 2*ServiceCount || snapshot.AttemptsStarted != 2*ServiceCount || snapshot.AttemptsCompleted != 2*ServiceCount {
		t.Errorf("attempt metrics = dispatched %d, started %d, completed %d; want %d each", snapshot.AttemptsDispatched, snapshot.AttemptsStarted, snapshot.AttemptsCompleted, 2*ServiceCount)
	}
	if !snapshot.HasPercentiles {
		t.Error("baseline metrics have no percentiles")
	}
}

func TestRescuePoolReservesNonBlockingCapacityAndResets(t *testing.T) {
	t.Parallel()

	pool, err := NewRescuePool(2, 2)
	if err != nil {
		t.Fatalf("NewRescuePool() returned an error: %v", err)
	}

	reservations := make([]*RescueReservation, 0, 4)
	wantWorkers := []string{
		"rescue-worker-1",
		"rescue-worker-1",
		"rescue-worker-2",
		"rescue-worker-2",
	}
	for index, wantWorker := range wantWorkers {
		reservation, ok := pool.TryReserve()
		if !ok {
			t.Fatalf("reservation %d was rejected", index)
		}
		if reservation.WorkerID() != wantWorker {
			t.Errorf("reservation %d worker = %q, want %q", index, reservation.WorkerID(), wantWorker)
		}
		reservations = append(reservations, reservation)
	}

	startedAt := time.Now()
	if reservation, ok := pool.TryReserve(); ok || reservation != nil {
		t.Fatal("TryReserve() succeeded after the pool was saturated")
	}
	if elapsed := time.Since(startedAt); elapsed >= 100*time.Millisecond {
		t.Errorf("saturated TryReserve() blocked for %s", elapsed)
	}

	snapshot := pool.Snapshot()
	if snapshot.TotalCapacity != 4 || snapshot.Occupancy != 4 || snapshot.PeakOccupancy != 4 {
		t.Errorf("pool capacity/current/peak = %d/%d/%d, want 4/4/4", snapshot.TotalCapacity, snapshot.Occupancy, snapshot.PeakOccupancy)
	}
	if snapshot.ReservationsAccepted != 4 || snapshot.ReservationsRejected != 1 {
		t.Errorf("accepted/rejected = %d/%d, want 4/1", snapshot.ReservationsAccepted, snapshot.ReservationsRejected)
	}
	for _, worker := range snapshot.Workers {
		if worker.Occupancy != 2 || worker.PeakOccupancy != 2 || worker.Capacity != 2 {
			t.Errorf("%s capacity/current/peak = %d/%d/%d, want 2/2/2", worker.WorkerID, worker.Capacity, worker.Occupancy, worker.PeakOccupancy)
		}
	}
	if err := pool.Reset(); err == nil {
		t.Error("Reset() succeeded with active reservations")
	}

	for _, reservation := range reservations {
		if !reservation.Release() {
			t.Errorf("first Release() failed for %s", reservation.WorkerID())
		}
		if reservation.Release() {
			t.Errorf("second Release() succeeded for %s", reservation.WorkerID())
		}
	}
	if snapshot = pool.Snapshot(); snapshot.Occupancy != 0 {
		t.Errorf("occupancy after release = %d, want 0", snapshot.Occupancy)
	}
	if err := pool.Reset(); err != nil {
		t.Fatalf("Reset() returned an error after release: %v", err)
	}
	snapshot = pool.Snapshot()
	if snapshot.Occupancy != 0 || snapshot.PeakOccupancy != 0 || snapshot.ReservationsAccepted != 0 || snapshot.ReservationsRejected != 0 {
		t.Errorf("reset snapshot retained state: %#v", snapshot)
	}
	for _, worker := range snapshot.Workers {
		if worker.Occupancy != 0 || worker.PeakOccupancy != 0 {
			t.Errorf("reset worker retained state: %#v", worker)
		}
	}
}

func TestSharedRescueWorkerExecutesEveryLogicalService(t *testing.T) {
	t.Parallel()

	pool, err := NewRescuePool(2, 1)
	if err != nil {
		t.Fatalf("NewRescuePool() returned an error: %v", err)
	}
	workerOneReservation, ok := pool.TryReserve()
	if !ok {
		t.Fatal("failed to reserve worker 1")
	}
	if workerOneReservation.WorkerID() != "rescue-worker-1" {
		t.Fatalf("first reservation worker = %q, want rescue-worker-1", workerOneReservation.WorkerID())
	}
	defer workerOneReservation.Release()

	for _, service := range Services() {
		reservation, ok := pool.TryReserve()
		if !ok {
			t.Fatalf("failed to reserve worker 2 for %s", service.ID)
		}
		if reservation.WorkerID() != "rescue-worker-2" {
			t.Fatalf("%s reservation worker = %q, want rescue-worker-2", service.ID, reservation.WorkerID())
		}

		response, err := reservation.Execute(context.Background(), service, 77, 0)
		if err != nil {
			t.Fatalf("worker 2 failed to execute %s: %v", service.ID, err)
		}
		if response.RequestID != 77 || response.ServiceID != service.ID || response.Status != ServiceResponseOK {
			t.Errorf("invalid %s response: %#v", service.ID, response)
		}
		if reservation.Release() {
			t.Errorf("%s reservation released twice after Execute", service.ID)
		}
	}

	if snapshot := pool.Snapshot(); snapshot.Occupancy != 1 {
		t.Errorf("occupancy with worker 1 held = %d, want 1", snapshot.Occupancy)
	}
}

func TestRescueExecutionReleasesCapacityOnCancellationAndError(t *testing.T) {
	t.Parallel()

	pool, err := NewRescuePool(1, 1)
	if err != nil {
		t.Fatalf("NewRescuePool() returned an error: %v", err)
	}

	reservation, ok := pool.TryReserve()
	if !ok {
		t.Fatal("failed to reserve capacity for cancellation test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = reservation.Execute(ctx, LogicalService{ID: UserService}, 1, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled Execute() error = %v, want context deadline exceeded", err)
	}
	if snapshot := pool.Snapshot(); snapshot.Occupancy != 0 {
		t.Errorf("occupancy after cancellation = %d, want 0", snapshot.Occupancy)
	}

	reservation, ok = pool.TryReserve()
	if !ok {
		t.Fatal("capacity was not reusable after cancellation")
	}
	_, err = reservation.Execute(context.Background(), LogicalService{ID: UserService}, 1, -time.Nanosecond)
	if err == nil {
		t.Fatal("Execute() returned nil error for a negative latency")
	}
	if snapshot := pool.Snapshot(); snapshot.Occupancy != 0 {
		t.Errorf("occupancy after handler error = %d, want 0", snapshot.Occupancy)
	}
	if _, err := reservation.Execute(context.Background(), LogicalService{ID: UserService}, 1, 0); err == nil {
		t.Error("reservation executed more than once")
	}
}

func TestRescuePoolConcurrentSaturationNeverExceedsCapacity(t *testing.T) {
	t.Parallel()

	const contenders = 50
	pool, err := NewRescuePool(2, 2)
	if err != nil {
		t.Fatalf("NewRescuePool() returned an error: %v", err)
	}

	start := make(chan struct{})
	results := make(chan *RescueReservation, contenders)
	var waitGroup sync.WaitGroup
	waitGroup.Add(contenders)
	for range contenders {
		go func() {
			defer waitGroup.Done()
			<-start
			reservation, ok := pool.TryReserve()
			if !ok {
				results <- nil
				return
			}
			results <- reservation
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)

	accepted := make([]*RescueReservation, 0, 4)
	for reservation := range results {
		if reservation != nil {
			accepted = append(accepted, reservation)
		}
	}
	if len(accepted) != 4 {
		t.Fatalf("accepted concurrent reservations = %d, want 4", len(accepted))
	}
	snapshot := pool.Snapshot()
	if snapshot.Occupancy != 4 || snapshot.PeakOccupancy != 4 {
		t.Errorf("concurrent current/peak occupancy = %d/%d, want 4/4", snapshot.Occupancy, snapshot.PeakOccupancy)
	}
	if snapshot.ReservationsRejected != contenders-len(accepted) {
		t.Errorf("rejected reservations = %d, want %d", snapshot.ReservationsRejected, contenders-len(accepted))
	}
	if snapshot.UtilizationPercent < 0 || snapshot.UtilizationPercent > 100 {
		t.Errorf("utilization = %g, want value in [0, 100]", snapshot.UtilizationPercent)
	}
	for _, reservation := range accepted {
		reservation.Release()
	}
	if snapshot = pool.Snapshot(); snapshot.Occupancy != 0 {
		t.Errorf("occupancy after concurrent release = %d, want 0", snapshot.Occupancy)
	}
}

func TestRescuePoolValidatesConfiguration(t *testing.T) {
	t.Parallel()

	if _, err := NewRescuePool(-1, 1); err == nil {
		t.Error("NewRescuePool() accepted a negative worker count")
	}
	if _, err := NewRescuePool(1, 0); err == nil {
		t.Error("NewRescuePool() accepted zero capacity")
	}
	pool, err := NewRescuePool(0, 1)
	if err != nil {
		t.Fatalf("NewRescuePool() rejected zero workers: %v", err)
	}
	if reservation, ok := pool.TryReserve(); ok || reservation != nil {
		t.Error("zero-worker pool accepted a reservation")
	}
	snapshot := pool.Snapshot()
	if snapshot.TotalCapacity != 0 || snapshot.ReservationsRejected != 1 {
		t.Errorf("zero-worker snapshot = %#v", snapshot)
	}
}

func TestProposalRescueWinsAndCancelsPrimary(t *testing.T) {
	t.Parallel()

	config := proposalTestConfig(1)
	plan := generatePlanForTest(t, config)
	setAllPlanLatencies(&plan, 1, 5*time.Millisecond, 5*time.Millisecond)
	setPlanLatencies(&plan, 1, RiskService, 200*time.Millisecond, 5*time.Millisecond)
	orchestrator := newProposalForTest(t, config, plan)

	execution, err := orchestrator.Execute(context.Background(), ClientRequest{ID: 1})
	if err != nil {
		t.Fatalf("Execute() returned an error: %v", err)
	}
	if execution.Response.Status != RequestStatusCompleted || len(execution.Response.Results) != ServiceCount {
		t.Fatalf("response status/results = %q/%d, want completed/%d", execution.Response.Status, len(execution.Response.Results), ServiceCount)
	}
	if execution.Rescues.Candidates != 1 || execution.Rescues.Activated != 1 || execution.Rescues.Successful != 1 {
		t.Errorf("rescue stats = %#v, want candidates=1 activated=1 successful=1", execution.Rescues)
	}
	if len(execution.Attempts) != ServiceCount+1 {
		t.Fatalf("attempt count = %d, want %d", len(execution.Attempts), ServiceCount+1)
	}

	primary := findAttempt(t, execution.Attempts, RiskService, AttemptTypePrimary)
	rescue := findAttempt(t, execution.Attempts, RiskService, AttemptTypeRescue)
	if primary.Status != AttemptStatusCancelledAfterStart || primary.CancellationReason != CancellationReasonHedgeLost || primary.Winner {
		t.Errorf("risk primary = %#v, want losing cancellation", primary)
	}
	if rescue.Status != AttemptStatusCompleted || !rescue.Winner {
		t.Errorf("risk rescue = %#v, want completed winner", rescue)
	}
	if rescue.Attempt.ServiceID != primary.Attempt.ServiceID {
		t.Errorf("rescue service = %s, primary service = %s", rescue.Attempt.ServiceID, primary.Attempt.ServiceID)
	}
	if response := execution.Response.Results[RiskService]; response.ServiceID != RiskService || response.RequestID != 1 {
		t.Errorf("invalid accepted risk response: %#v", response)
	}
	assertProposalIdle(t, orchestrator)

	metrics := orchestrator.Metrics()
	if metrics.RescueCandidates != 1 || metrics.RescuesActivated != 1 || metrics.RescuesSuccessful != 1 {
		t.Errorf("proposal rescue metrics = candidates %d, activated %d, successful %d", metrics.RescueCandidates, metrics.RescuesActivated, metrics.RescuesSuccessful)
	}
}

func TestProposalPrimaryWinsAndCancelsRescue(t *testing.T) {
	t.Parallel()

	config := proposalTestConfig(1)
	plan := generatePlanForTest(t, config)
	setAllPlanLatencies(&plan, 1, 5*time.Millisecond, 5*time.Millisecond)
	setPlanLatencies(&plan, 1, PaymentService, 50*time.Millisecond, 200*time.Millisecond)
	orchestrator := newProposalForTest(t, config, plan)

	execution, err := orchestrator.Execute(context.Background(), ClientRequest{ID: 1})
	if err != nil {
		t.Fatalf("Execute() returned an error: %v", err)
	}
	primary := findAttempt(t, execution.Attempts, PaymentService, AttemptTypePrimary)
	rescue := findAttempt(t, execution.Attempts, PaymentService, AttemptTypeRescue)
	if primary.Status != AttemptStatusCompleted || !primary.Winner {
		t.Errorf("payment primary = %#v, want completed winner", primary)
	}
	if rescue.Status != AttemptStatusCancelledAfterStart || rescue.CancellationReason != CancellationReasonHedgeLost || rescue.Winner {
		t.Errorf("payment rescue = %#v, want losing cancellation", rescue)
	}
	if execution.Rescues.Successful != 0 || execution.Rescues.Activated != 1 {
		t.Errorf("rescue stats = %#v, want activated=1 successful=0", execution.Rescues)
	}
	assertProposalIdle(t, orchestrator)
}

func TestProposalHonorsRescueBudgetAndOneRescuePerService(t *testing.T) {
	t.Parallel()

	config := proposalTestConfig(1)
	config.RescueBudgetPerRequest = 3
	plan := generatePlanForTest(t, config)
	setAllPlanLatencies(&plan, 1, 120*time.Millisecond, 5*time.Millisecond)
	orchestrator := newProposalForTest(t, config, plan)

	execution, err := orchestrator.Execute(context.Background(), ClientRequest{ID: 1})
	if err != nil {
		t.Fatalf("Execute() returned an error: %v", err)
	}
	if execution.Rescues.Candidates != ServiceCount {
		t.Errorf("rescue candidates = %d, want %d", execution.Rescues.Candidates, ServiceCount)
	}
	if execution.Rescues.Activated != config.RescueBudgetPerRequest {
		t.Errorf("rescues activated = %d, want %d", execution.Rescues.Activated, config.RescueBudgetPerRequest)
	}
	if execution.Rescues.Successful != config.RescueBudgetPerRequest {
		t.Errorf("rescues successful = %d, want %d", execution.Rescues.Successful, config.RescueBudgetPerRequest)
	}
	if len(execution.Attempts) != ServiceCount+config.RescueBudgetPerRequest {
		t.Errorf("attempt count = %d, want %d", len(execution.Attempts), ServiceCount+config.RescueBudgetPerRequest)
	}

	rescuesByService := make(map[ServiceID]int)
	winnersByService := make(map[ServiceID]int)
	rescuedServices := make([]ServiceID, 0, config.RescueBudgetPerRequest)
	for _, record := range execution.Attempts {
		if record.Attempt.Type == AttemptTypeRescue {
			rescuesByService[record.Attempt.ServiceID]++
			rescuedServices = append(rescuedServices, record.Attempt.ServiceID)
		}
		if record.Winner {
			winnersByService[record.Attempt.ServiceID]++
		}
	}
	for _, serviceID := range ServiceIDs() {
		if rescuesByService[serviceID] > 1 {
			t.Errorf("%s received %d rescues", serviceID, rescuesByService[serviceID])
		}
		if winnersByService[serviceID] != 1 {
			t.Errorf("%s winner count = %d, want 1", serviceID, winnersByService[serviceID])
		}
	}
	wantRescued := []ServiceID{UserService, ProductService, InventoryService}
	for index, serviceID := range wantRescued {
		if rescuedServices[index] != serviceID {
			t.Errorf("rescued service %d = %s, want %s", index, rescuedServices[index], serviceID)
		}
	}
	assertProposalIdle(t, orchestrator)
}

func TestProposalSkipsRescueWithoutCapacity(t *testing.T) {
	t.Parallel()

	config := proposalTestConfig(1)
	config.RescueWorkers = 1
	config.CapacityPerInstance = 1
	plan := generatePlanForTest(t, config)
	setAllPlanLatencies(&plan, 1, 60*time.Millisecond, 2*time.Millisecond)
	orchestrator := newProposalForTest(t, config, plan)

	blocker, ok := orchestrator.rescuePool.TryReserve()
	if !ok {
		t.Fatal("failed to saturate rescue pool")
	}
	execution, err := orchestrator.Execute(context.Background(), ClientRequest{ID: 1})
	if err != nil {
		t.Fatalf("Execute() returned an error: %v", err)
	}
	if execution.Response.Status != RequestStatusCompleted {
		t.Errorf("response status = %q, want completed", execution.Response.Status)
	}
	if execution.Rescues.Candidates != ServiceCount || execution.Rescues.Activated != 0 || execution.Rescues.SkippedNoCapacity != ServiceCount {
		t.Errorf("rescue stats = %#v, want %d candidates and capacity skips", execution.Rescues, ServiceCount)
	}
	if len(execution.Attempts) != ServiceCount {
		t.Errorf("attempt count = %d, want only %d primaries", len(execution.Attempts), ServiceCount)
	}
	if !blocker.Release() {
		t.Error("failed to release saturation reservation")
	}
	assertProposalIdle(t, orchestrator)
}

func TestProposalGlobalTimeoutCancelsAllWork(t *testing.T) {
	t.Parallel()

	config := proposalTestConfig(1)
	config.Timeout = 60 * time.Millisecond
	plan := generatePlanForTest(t, config)
	setAllPlanLatencies(&plan, 1, time.Second, time.Second)
	orchestrator := newProposalForTest(t, config, plan)

	execution, err := orchestrator.Execute(context.Background(), ClientRequest{ID: 1})
	if err != nil {
		t.Fatalf("Execute() returned an error: %v", err)
	}
	if execution.Response.Status != RequestStatusTimedOut {
		t.Fatalf("response status = %q, want timed_out", execution.Response.Status)
	}
	if execution.Rescues.Activated != config.RescueBudgetPerRequest {
		t.Errorf("rescues activated = %d, want %d", execution.Rescues.Activated, config.RescueBudgetPerRequest)
	}
	if len(execution.Attempts) != ServiceCount+config.RescueBudgetPerRequest {
		t.Errorf("attempt count = %d, want %d", len(execution.Attempts), ServiceCount+config.RescueBudgetPerRequest)
	}
	for _, record := range execution.Attempts {
		if record.Status != AttemptStatusCancelledAfterStart {
			t.Errorf("%s status = %q, want cancelled_after_start", record.Attempt.ID, record.Status)
		}
		if record.CancellationReason != CancellationReasonRequestTimeout {
			t.Errorf("%s cancellation reason = %q, want request_timeout", record.Attempt.ID, record.CancellationReason)
		}
		if record.Winner {
			t.Errorf("timed-out attempt %s marked as winner", record.Attempt.ID)
		}
	}
	assertProposalIdle(t, orchestrator)
}

func TestProposalDoesNotHedgeFastPrimariesAndReusesPrimaryPlan(t *testing.T) {
	t.Parallel()

	config := proposalTestConfig(1)
	plan := generatePlanForTest(t, config)
	setAllPlanLatencies(&plan, 1, 2*time.Millisecond, 100*time.Millisecond)
	baseline, err := NewBaselineOrchestrator(config, plan)
	if err != nil {
		t.Fatalf("NewBaselineOrchestrator() returned an error: %v", err)
	}
	proposal := newProposalForTest(t, config, plan)

	baselineExecution, err := baseline.Execute(context.Background(), ClientRequest{ID: 1})
	if err != nil {
		t.Fatalf("baseline Execute() returned an error: %v", err)
	}
	proposalExecution, err := proposal.Execute(context.Background(), ClientRequest{ID: 1})
	if err != nil {
		t.Fatalf("proposal Execute() returned an error: %v", err)
	}
	if proposalExecution.Rescues != (RescueRequestStats{}) {
		t.Errorf("fast primaries produced rescue stats: %#v", proposalExecution.Rescues)
	}
	if len(proposalExecution.Attempts) != ServiceCount {
		t.Errorf("proposal attempt count = %d, want %d", len(proposalExecution.Attempts), ServiceCount)
	}
	for index := 0; index < ServiceCount; index++ {
		baselineAttempt := baselineExecution.Attempts[index].Attempt
		proposalAttempt := proposalExecution.Attempts[index].Attempt
		if baselineAttempt.ServiceID != proposalAttempt.ServiceID || baselineAttempt.PlannedLatency != proposalAttempt.PlannedLatency || baselineAttempt.PlannedSlow != proposalAttempt.PlannedSlow {
			t.Errorf("primary plan differs at index %d: baseline %#v, proposal %#v", index, baselineAttempt, proposalAttempt)
		}
	}
}

func TestRescueCandidateTieBreakIsReproducible(t *testing.T) {
	t.Parallel()

	dispatchedAt := time.Now().Add(-time.Second)
	buildStates := func() map[ServiceID]*proposalServiceState {
		states := make(map[ServiceID]*proposalServiceState, ServiceCount)
		for index, service := range Services() {
			states[service.ID] = &proposalServiceState{
				service:      service,
				dispatchedAt: dispatchedAt,
				tieKey:       rescueTieKey(2026, 1, index),
			}
		}
		return states
	}

	first := eligibleRescueCandidates(buildStates(), 50*time.Millisecond, time.Now())
	second := eligibleRescueCandidates(buildStates(), 50*time.Millisecond, time.Now())
	if len(first) != ServiceCount || len(second) != ServiceCount {
		t.Fatalf("candidate lengths = %d/%d, want %d", len(first), len(second), ServiceCount)
	}
	seen := make(map[ServiceID]bool, ServiceCount)
	for index := range first {
		if first[index].service.ID != second[index].service.ID {
			t.Errorf("tie order differs at %d: %s != %s", index, first[index].service.ID, second[index].service.ID)
		}
		if seen[first[index].service.ID] {
			t.Errorf("duplicate tied candidate %s", first[index].service.ID)
		}
		seen[first[index].service.ID] = true
	}
}

func proposalTestConfig(requestCount int) Config {
	config := fixedLatencyConfig(requestCount, 5*time.Millisecond)
	config.HedgeThreshold = 20 * time.Millisecond
	config.RescueWorkers = 2
	config.CapacityPerInstance = 5
	config.RescueBudgetPerRequest = 3
	config.MaxRescueAttemptsPerService = 1
	config.Timeout = 2 * time.Second
	return config
}

func newProposalForTest(t *testing.T, config Config, plan LatencyPlan) *ProposalOrchestrator {
	t.Helper()

	orchestrator, err := NewProposalOrchestrator(config, plan)
	if err != nil {
		t.Fatalf("NewProposalOrchestrator() returned an error: %v", err)
	}
	return orchestrator
}

func setAllPlanLatencies(plan *LatencyPlan, requestID int, primary, rescue time.Duration) {
	for _, serviceID := range ServiceIDs() {
		setPlanLatencies(plan, requestID, serviceID, primary, rescue)
	}
}

func setPlanLatencies(
	plan *LatencyPlan,
	requestID int,
	serviceID ServiceID,
	primary time.Duration,
	rescue time.Duration,
) {
	plan.entries[latencyPlanKey{requestID: requestID, serviceID: serviceID}] = ServiceLatencies{
		Primary: LatencySample{Duration: primary},
		Rescue:  LatencySample{Duration: rescue},
	}
}

func findAttempt(
	t *testing.T,
	records []AttemptRecord,
	serviceID ServiceID,
	attemptType AttemptType,
) AttemptRecord {
	t.Helper()

	for _, record := range records {
		if record.Attempt.ServiceID == serviceID && record.Attempt.Type == attemptType {
			return record
		}
	}
	t.Fatalf("missing %s attempt for %s", attemptType, serviceID)
	return AttemptRecord{}
}

func assertProposalIdle(t *testing.T, orchestrator *ProposalOrchestrator) {
	t.Helper()

	pool := orchestrator.RescuePoolSnapshot()
	if pool.Occupancy != 0 {
		t.Errorf("rescue pool occupancy = %d, want 0", pool.Occupancy)
	}
	for _, serviceID := range ServiceIDs() {
		current, _, ok := orchestrator.PrimaryOccupancy(serviceID)
		if !ok {
			t.Errorf("missing proposal primary %s", serviceID)
			continue
		}
		if current != 0 {
			t.Errorf("proposal primary %s occupancy = %d, want 0", serviceID, current)
		}
	}
}

func TestArchitecturesUnderConcurrentDeterministicWorkload(t *testing.T) {
	t.Parallel()

	const (
		requestCount      = 30
		clientConcurrency = 10
	)
	config := proposalTestConfig(requestCount)
	config.MaxClientConcurrency = clientConcurrency
	config.CapacityPerInstance = 3
	config.HedgeThreshold = 8 * time.Millisecond
	config.Timeout = 2 * time.Second
	plan := generatePlanForTest(t, config)
	for requestID := 1; requestID <= requestCount; requestID++ {
		for serviceIndex, serviceID := range ServiceIDs() {
			primaryLatency := 2 * time.Millisecond
			if (requestID+serviceIndex)%5 == 0 {
				primaryLatency = 35 * time.Millisecond
			}
			setPlanLatencies(&plan, requestID, serviceID, primaryLatency, 3*time.Millisecond)
		}
	}

	baseline, err := NewBaselineOrchestrator(config, plan)
	if err != nil {
		t.Fatalf("NewBaselineOrchestrator() returned an error: %v", err)
	}
	proposal := newProposalForTest(t, config, plan)
	secondProposal := newProposalForTest(t, config, plan)

	baselineExecutions := executeConcurrentRequestsForTest(t, baseline, requestCount, clientConcurrency)
	proposalExecutions := executeConcurrentRequestsForTest(t, proposal, requestCount, clientConcurrency)
	secondProposalExecutions := executeConcurrentRequestsForTest(t, secondProposal, requestCount, clientConcurrency)

	for requestIndex := 0; requestIndex < requestCount; requestIndex++ {
		baselineExecution := baselineExecutions[requestIndex]
		proposalExecution := proposalExecutions[requestIndex]
		secondProposalExecution := secondProposalExecutions[requestIndex]
		assertCompletedExecutionIntegrity(t, baselineExecution, false, config)
		assertCompletedExecutionIntegrity(t, proposalExecution, true, config)
		assertCompletedExecutionIntegrity(t, secondProposalExecution, true, config)

		for _, serviceID := range ServiceIDs() {
			baselineResponse := baselineExecution.Response.Results[serviceID]
			proposalResponse := proposalExecution.Response.Results[serviceID]
			secondProposalResponse := secondProposalExecution.Response.Results[serviceID]
			if baselineResponse != proposalResponse {
				t.Errorf("request %d service %s differs between architectures: %#v != %#v", requestIndex+1, serviceID, baselineResponse, proposalResponse)
			}
			if proposalResponse != secondProposalResponse {
				t.Errorf("request %d service %s is not reproducible: %#v != %#v", requestIndex+1, serviceID, proposalResponse, secondProposalResponse)
			}
		}
	}

	assertMetricsInvariants(t, baseline.Metrics(), requestCount)
	assertMetricsInvariants(t, proposal.Metrics(), requestCount)
	assertMetricsInvariants(t, secondProposal.Metrics(), requestCount)
	for _, serviceID := range ServiceIDs() {
		baselineCurrent, baselinePeak, _ := baseline.PrimaryOccupancy(serviceID)
		if baselineCurrent != 0 || baselinePeak > config.CapacityPerInstance {
			t.Errorf("baseline %s current/peak = %d/%d, capacity %d", serviceID, baselineCurrent, baselinePeak, config.CapacityPerInstance)
		}
		proposalCurrent, proposalPeak, _ := proposal.PrimaryOccupancy(serviceID)
		if proposalCurrent != 0 || proposalPeak > config.CapacityPerInstance {
			t.Errorf("proposal %s current/peak = %d/%d, capacity %d", serviceID, proposalCurrent, proposalPeak, config.CapacityPerInstance)
		}
	}
	pool := proposal.RescuePoolSnapshot()
	if pool.Occupancy != 0 || pool.PeakOccupancy > pool.TotalCapacity {
		t.Errorf("proposal pool current/peak/capacity = %d/%d/%d", pool.Occupancy, pool.PeakOccupancy, pool.TotalCapacity)
	}
	assertProposalIdle(t, proposal)
	assertProposalIdle(t, secondProposal)
}

// This test intentionally does not call t.Parallel. Other top-level parallel
// tests remain paused, making the process goroutine count meaningful.
func TestNoStableGoroutineLeakAfterConcurrentWorkloads(t *testing.T) {
	const (
		requestCount      = 24
		clientConcurrency = 8
	)
	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()

	config := proposalTestConfig(requestCount)
	config.MaxClientConcurrency = clientConcurrency
	config.CapacityPerInstance = 2
	config.HedgeThreshold = 5 * time.Millisecond
	config.Timeout = time.Second
	plan := generatePlanForTest(t, config)
	for requestID := 1; requestID <= requestCount; requestID++ {
		for serviceIndex, serviceID := range ServiceIDs() {
			primaryLatency := 2 * time.Millisecond
			if (requestID+serviceIndex)%3 == 0 {
				primaryLatency = 20 * time.Millisecond
			}
			setPlanLatencies(&plan, requestID, serviceID, primaryLatency, 2*time.Millisecond)
		}
	}

	for iteration := 0; iteration < 3; iteration++ {
		baseline, err := NewBaselineOrchestrator(config, plan)
		if err != nil {
			t.Fatalf("iteration %d baseline constructor error: %v", iteration, err)
		}
		proposal := newProposalForTest(t, config, plan)
		executeConcurrentRequestsForTest(t, baseline, requestCount, clientConcurrency)
		executeConcurrentRequestsForTest(t, proposal, requestCount, clientConcurrency)
		assertProposalIdle(t, proposal)
		for _, serviceID := range ServiceIDs() {
			current, _, _ := baseline.PrimaryOccupancy(serviceID)
			if current != 0 {
				t.Fatalf("iteration %d baseline %s occupancy = %d, want 0", iteration, serviceID, current)
			}
		}
	}

	const allowedRuntimeVariation = 3
	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.GC()
		current := runtime.NumGoroutine()
		if current <= baselineGoroutines+allowedRuntimeVariation {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines did not return to a stable level: before=%d after=%d", baselineGoroutines, current)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type requestExecutor interface {
	Execute(context.Context, ClientRequest) (RequestExecution, error)
}

func executeConcurrentRequestsForTest(
	t *testing.T,
	executor requestExecutor,
	requestCount int,
	concurrency int,
) []RequestExecution {
	t.Helper()

	executions := make([]RequestExecution, requestCount)
	errorsByRequest := make([]error, requestCount)
	clientSlots := make(chan struct{}, concurrency)
	var waitGroup sync.WaitGroup
	waitGroup.Add(requestCount)
	for requestID := 1; requestID <= requestCount; requestID++ {
		go func() {
			defer waitGroup.Done()
			clientSlots <- struct{}{}
			defer func() { <-clientSlots }()
			executions[requestID-1], errorsByRequest[requestID-1] = executor.Execute(
				context.Background(),
				ClientRequest{ID: requestID},
			)
		}()
	}
	waitGroup.Wait()

	for requestIndex, err := range errorsByRequest {
		if err != nil {
			t.Fatalf("request %d returned an error: %v", requestIndex+1, err)
		}
	}
	return executions
}

func assertCompletedExecutionIntegrity(
	t *testing.T,
	execution RequestExecution,
	proposal bool,
	config Config,
) {
	t.Helper()

	if execution.Response.Status != RequestStatusCompleted {
		t.Errorf("request %d status = %q, want completed", execution.Response.RequestID, execution.Response.Status)
		return
	}
	if len(execution.Response.Results) != ServiceCount {
		t.Errorf("request %d result count = %d, want %d", execution.Response.RequestID, len(execution.Response.Results), ServiceCount)
	}
	if execution.Rescues.Activated > config.RescueBudgetPerRequest {
		t.Errorf("request %d activated %d rescues, budget %d", execution.Response.RequestID, execution.Rescues.Activated, config.RescueBudgetPerRequest)
	}

	primaryByService := make(map[ServiceID]int, ServiceCount)
	rescueByService := make(map[ServiceID]int, ServiceCount)
	winnerByService := make(map[ServiceID]int, ServiceCount)
	for _, record := range execution.Attempts {
		switch record.Attempt.Type {
		case AttemptTypePrimary:
			primaryByService[record.Attempt.ServiceID]++
		case AttemptTypeRescue:
			rescueByService[record.Attempt.ServiceID]++
		}
		if record.Winner {
			winnerByService[record.Attempt.ServiceID]++
		}
		switch record.Status {
		case AttemptStatusCompleted, AttemptStatusCancelledBeforeStart, AttemptStatusCancelledAfterStart:
		default:
			t.Errorf("request %d attempt %s has non-terminal status %q", execution.Response.RequestID, record.Attempt.ID, record.Status)
		}
	}

	for _, serviceID := range ServiceIDs() {
		if primaryByService[serviceID] != 1 {
			t.Errorf("request %d service %s primary count = %d, want 1", execution.Response.RequestID, serviceID, primaryByService[serviceID])
		}
		if rescueByService[serviceID] > 1 {
			t.Errorf("request %d service %s rescue count = %d, want at most 1", execution.Response.RequestID, serviceID, rescueByService[serviceID])
		}
		if !proposal && rescueByService[serviceID] != 0 {
			t.Errorf("baseline request %d service %s has a rescue", execution.Response.RequestID, serviceID)
		}
		if winnerByService[serviceID] != 1 {
			t.Errorf("request %d service %s winner count = %d, want 1", execution.Response.RequestID, serviceID, winnerByService[serviceID])
		}
		response := execution.Response.Results[serviceID]
		if response.RequestID != execution.Response.RequestID || response.ServiceID != serviceID || response.Status != ServiceResponseOK {
			t.Errorf("request %d invalid response for %s: %#v", execution.Response.RequestID, serviceID, response)
		}
	}
}

func assertMetricsInvariants(t *testing.T, metrics MetricsSnapshot, requestCount int) {
	t.Helper()

	if metrics.ClientRequestsReceived != requestCount || metrics.ClientRequestsCompleted != requestCount || metrics.ClientRequestsTimedOut != 0 {
		t.Errorf("%s client metrics = received %d completed %d timed-out %d; want %d/%d/0", metrics.Architecture, metrics.ClientRequestsReceived, metrics.ClientRequestsCompleted, metrics.ClientRequestsTimedOut, requestCount, requestCount)
	}
	if len(metrics.Latencies) != requestCount || !metrics.HasPercentiles {
		t.Errorf("%s latency metrics = count %d, percentiles %t; want %d/true", metrics.Architecture, len(metrics.Latencies), metrics.HasPercentiles, requestCount)
	}
	if metrics.AttemptsDispatched != metrics.AttemptsCompleted+metrics.AttemptsCancelledBeforeStart+metrics.AttemptsCancelledAfterStart {
		t.Errorf("%s dispatched identity failed: %d != %d + %d + %d", metrics.Architecture, metrics.AttemptsDispatched, metrics.AttemptsCompleted, metrics.AttemptsCancelledBeforeStart, metrics.AttemptsCancelledAfterStart)
	}
	if metrics.AttemptsStarted != metrics.AttemptsCompleted+metrics.AttemptsCancelledAfterStart {
		t.Errorf("%s started identity failed: %d != %d + %d", metrics.Architecture, metrics.AttemptsStarted, metrics.AttemptsCompleted, metrics.AttemptsCancelledAfterStart)
	}
	if metrics.RescuesSuccessful > metrics.RescuesActivated || metrics.RescuesActivated > metrics.RescueCandidates {
		t.Errorf("%s rescue metrics invalid: candidates %d activated %d successful %d", metrics.Architecture, metrics.RescueCandidates, metrics.RescuesActivated, metrics.RescuesSuccessful)
	}
}

func TestScenarioPresets(t *testing.T) {
	t.Parallel()

	base := DefaultConfig()
	tests := []struct {
		name        string
		wantCount   int
		wantRequest []int
		wantClients []int
	}{
		{name: "functional", wantCount: 1, wantRequest: []int{100}, wantClients: []int{5}},
		{name: "main", wantCount: 1, wantRequest: []int{2000}, wantClients: []int{10}},
		{name: "load", wantCount: 1, wantRequest: []int{2000}, wantClients: []int{30}},
		{name: "extended", wantCount: 1, wantRequest: []int{5000}, wantClients: []int{10}},
		{name: "all", wantCount: 3, wantRequest: []int{100, 2000, 2000}, wantClients: []int{5, 10, 30}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			scenarios, err := PresetScenarios(test.name, base)
			if err != nil {
				t.Fatalf("PresetScenarios() returned an error: %v", err)
			}
			if len(scenarios) != test.wantCount {
				t.Fatalf("scenario count = %d, want %d", len(scenarios), test.wantCount)
			}
			for index, scenario := range scenarios {
				if scenario.Config.RequestCount != test.wantRequest[index] || scenario.Config.MaxClientConcurrency != test.wantClients[index] {
					t.Errorf("scenario %d requests/concurrency = %d/%d, want %d/%d", index, scenario.Config.RequestCount, scenario.Config.MaxClientConcurrency, test.wantRequest[index], test.wantClients[index])
				}
			}
		})
	}
	if _, err := PresetScenarios("unknown", base); err == nil {
		t.Error("PresetScenarios() accepted an unknown scenario")
	}
}

func TestCLIOptionsOverrideAllConfigurableParameters(t *testing.T) {
	t.Parallel()

	options, err := parseCLIOptions([]string{
		"-scenario", "custom",
		"-requests", "7",
		"-concurrency", "3",
		"-slow-probability", "0.2",
		"-normal-min", "1ms",
		"-normal-max", "2ms",
		"-slow-min", "3ms",
		"-slow-max", "4ms",
		"-hedge-threshold", "5ms",
		"-rescue-workers", "4",
		"-capacity", "6",
		"-rescue-budget", "2",
		"-max-rescues-per-service", "1",
		"-timeout", "2s",
		"-seed", "99",
		"-verbose",
		"-log-request", "4",
		"-output", "custom-results",
		"-overwrite",
	}, os.Stderr)
	if err != nil {
		t.Fatalf("parseCLIOptions() returned an error: %v", err)
	}
	if len(options.Scenarios) != 1 {
		t.Fatalf("scenario count = %d, want 1", len(options.Scenarios))
	}
	config := options.Scenarios[0].Config
	if config.RequestCount != 7 || config.MaxClientConcurrency != 3 || config.SlowProbability != 0.2 ||
		config.NormalLatencyMin != time.Millisecond || config.NormalLatencyMax != 2*time.Millisecond ||
		config.SlowLatencyMin != 3*time.Millisecond || config.SlowLatencyMax != 4*time.Millisecond ||
		config.HedgeThreshold != 5*time.Millisecond || config.RescueWorkers != 4 ||
		config.CapacityPerInstance != 6 || config.RescueBudgetPerRequest != 2 ||
		config.MaxRescueAttemptsPerService != 1 || config.Timeout != 2*time.Second || config.Seed != 99 ||
		!config.Verbose || config.LogRequestID != 4 {
		t.Errorf("unexpected parsed config: %#v", config)
	}
	if options.OutputDirectory != "custom-results" || !options.Overwrite {
		t.Errorf("output options = %q/%t, want custom-results/true", options.OutputDirectory, options.Overwrite)
	}
	if _, err := parseCLIOptions([]string{"-scenario", "unknown"}, os.Stderr); err == nil {
		t.Error("parseCLIOptions() accepted an unknown scenario")
	}
}

type concurrencyTrackingExecutor struct {
	mu      sync.Mutex
	current int
	peak    int
	delay   time.Duration
}

func (e *concurrencyTrackingExecutor) Execute(
	_ context.Context,
	request ClientRequest,
) (RequestExecution, error) {
	e.mu.Lock()
	e.current++
	if e.current > e.peak {
		e.peak = e.current
	}
	e.mu.Unlock()
	time.Sleep(e.delay)
	e.mu.Lock()
	e.current--
	e.mu.Unlock()
	return RequestExecution{Response: AggregatedResponse{
		RequestID: request.ID,
		Status:    RequestStatusCompleted,
	}}, nil
}

func TestExperimentRunnerLimitsClientConcurrency(t *testing.T) {
	t.Parallel()

	executor := &concurrencyTrackingExecutor{delay: 10 * time.Millisecond}
	executions, err := runClientRequests(context.Background(), executor, 20, 4)
	if err != nil {
		t.Fatalf("runClientRequests() returned an error: %v", err)
	}
	if len(executions) != 20 {
		t.Errorf("execution count = %d, want 20", len(executions))
	}
	executor.mu.Lock()
	current, peak := executor.current, executor.peak
	executor.mu.Unlock()
	if current != 0 || peak != 4 {
		t.Errorf("client current/peak concurrency = %d/%d, want 0/4", current, peak)
	}
}

func TestRunExperimentsAndExportCSV(t *testing.T) {
	t.Parallel()

	config := proposalTestConfig(6)
	config.MaxClientConcurrency = 3
	config.SlowProbability = 0
	config.NormalLatencyMin = 0
	config.NormalLatencyMax = 0
	config.HedgeThreshold = time.Millisecond
	scenarios := []Scenario{{Name: "csv-test", Config: config}}
	results, err := RunExperiments(context.Background(), scenarios)
	if err != nil {
		t.Fatalf("RunExperiments() returned an error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("result count = %d, want 1", len(results))
	}
	result := results[0]
	if len(result.Baseline.Executions) != config.RequestCount || len(result.Proposal.Executions) != config.RequestCount {
		t.Fatalf("baseline/proposal execution counts = %d/%d, want %d each", len(result.Baseline.Executions), len(result.Proposal.Executions), config.RequestCount)
	}
	for requestIndex := range result.Baseline.Executions {
		baselineAttempts := result.Baseline.Executions[requestIndex].Attempts
		proposalAttempts := result.Proposal.Executions[requestIndex].Attempts
		if len(baselineAttempts) != ServiceCount {
			t.Fatalf("request %d baseline attempt count = %d, want %d", requestIndex+1, len(baselineAttempts), ServiceCount)
		}
		proposalPrimaries := make(map[ServiceID]Attempt, ServiceCount)
		for _, record := range proposalAttempts {
			if record.Attempt.Type == AttemptTypePrimary {
				proposalPrimaries[record.Attempt.ServiceID] = record.Attempt
			}
		}
		if len(proposalPrimaries) != ServiceCount {
			t.Fatalf("request %d proposal primary count = %d, want %d", requestIndex+1, len(proposalPrimaries), ServiceCount)
		}
		for _, baselineRecord := range baselineAttempts {
			baselineAttempt := baselineRecord.Attempt
			proposalAttempt := proposalPrimaries[baselineAttempt.ServiceID]
			if baselineAttempt.PlannedLatency != proposalAttempt.PlannedLatency || baselineAttempt.PlannedSlow != proposalAttempt.PlannedSlow {
				t.Errorf("request %d primary plan differs for %s", requestIndex+1, baselineAttempt.ServiceID)
			}
		}
	}

	outputDirectory := t.TempDir()
	if err := ExportCSV(results, outputDirectory, false); err != nil {
		t.Fatalf("ExportCSV() returned an error: %v", err)
	}
	baselineCSV := readCSVForTest(t, filepath.Join(outputDirectory, "baseline.csv"))
	proposalCSV := readCSVForTest(t, filepath.Join(outputDirectory, "proposal.csv"))
	attemptsCSV := readCSVForTest(t, filepath.Join(outputDirectory, "attempts.csv"))
	summaryCSV := readCSVForTest(t, filepath.Join(outputDirectory, "summary.csv"))
	if len(baselineCSV) != config.RequestCount+1 || len(proposalCSV) != config.RequestCount+1 {
		t.Errorf("request CSV row counts = %d/%d, want %d each", len(baselineCSV), len(proposalCSV), config.RequestCount+1)
	}
	wantAttemptRows := 1 + result.Baseline.Metrics.AttemptsDispatched + result.Proposal.Metrics.AttemptsDispatched
	if len(attemptsCSV) != wantAttemptRows {
		t.Errorf("attempt CSV rows = %d, want %d", len(attemptsCSV), wantAttemptRows)
	}
	if len(summaryCSV) != 3 {
		t.Errorf("summary CSV rows = %d, want 3", len(summaryCSV))
	}
	if got := baselineCSV[0]; len(got) != 15 || got[4] != "received_at_peru" || got[14] != "rescues_successful" {
		t.Errorf("unexpected baseline header: %v", got)
	}
	if timestamp := baselineCSV[1][4]; !strings.HasSuffix(timestamp, "-05:00") {
		t.Errorf("Peru timestamp = %q, want -05:00 suffix", timestamp)
	}
	if summaryCSV[1][2] != string(ArchitectureBaseline) || summaryCSV[1][19] != "10" {
		t.Errorf("baseline summary architecture/instances = %q/%q, want baseline/10", summaryCSV[1][2], summaryCSV[1][19])
	}
	if summaryCSV[2][2] != string(ArchitectureProposal) || summaryCSV[2][19] != "12" {
		t.Errorf("proposal summary architecture/instances = %q/%q, want proposal/12", summaryCSV[2][2], summaryCSV[2][19])
	}
	if err := ExportCSV(results, outputDirectory, false); err == nil {
		t.Error("ExportCSV() overwrote existing files without permission")
	}
	if err := ExportCSV(results, outputDirectory, true); err != nil {
		t.Errorf("ExportCSV(overwrite=true) returned an error: %v", err)
	}
}

func TestRunExperimentsRejectsEmptyScenarioList(t *testing.T) {
	t.Parallel()

	if _, err := RunExperiments(context.Background(), nil); err == nil {
		t.Error("RunExperiments() accepted an empty scenario list")
	}
}

func readCSVForTest(t *testing.T, path string) [][]string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return records
}
