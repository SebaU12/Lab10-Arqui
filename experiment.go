package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Scenario identifies a reproducible set of configuration values.
type Scenario struct {
	Name   string
	Config Config
}

type ArchitectureExperiment struct {
	Architecture Architecture
	StartedAt    time.Time
	FinishedAt   time.Time
	WallElapsed  time.Duration
	Executions   []RequestExecution
	Metrics      MetricsSnapshot
	RescuePool   RescuePoolSnapshot
}

type ExperimentResult struct {
	ExperimentID string
	Scenario     Scenario
	Baseline     ArchitectureExperiment
	Proposal     ArchitectureExperiment
}

type clientRequestExecutor interface {
	Execute(context.Context, ClientRequest) (RequestExecution, error)
}

func PresetScenarios(name string, base Config) ([]Scenario, error) {
	switch name {
	case "functional":
		base.RequestCount = 100
		base.MaxClientConcurrency = 5
		return []Scenario{{Name: "functional", Config: base}}, nil
	case "main":
		base.RequestCount = 2000
		base.MaxClientConcurrency = 10
		return []Scenario{{Name: "main", Config: base}}, nil
	case "load":
		base.RequestCount = 2000
		base.MaxClientConcurrency = 30
		return []Scenario{{Name: "load", Config: base}}, nil
	case "extended":
		base.RequestCount = 5000
		base.MaxClientConcurrency = 10
		return []Scenario{{Name: "extended", Config: base}}, nil
	case "custom":
		return []Scenario{{Name: "custom", Config: base}}, nil
	case "all":
		functional := base
		functional.RequestCount = 100
		functional.MaxClientConcurrency = 5
		mainScenario := base
		mainScenario.RequestCount = 2000
		mainScenario.MaxClientConcurrency = 10
		load := base
		load.RequestCount = 2000
		load.MaxClientConcurrency = 30
		return []Scenario{
			{Name: "functional", Config: functional},
			{Name: "main", Config: mainScenario},
			{Name: "load", Config: load},
		}, nil
	default:
		return nil, fmt.Errorf("unknown scenario %q", name)
	}
}

// RunExperiments executes each scenario completely before starting the next.
// Within a scenario, baseline completes before a fresh proposal is created.
func RunExperiments(ctx context.Context, scenarios []Scenario) ([]ExperimentResult, error) {
	if len(scenarios) == 0 {
		return nil, errors.New("run experiments: no scenarios configured")
	}

	experimentID, err := newExperimentID(time.Now())
	if err != nil {
		return nil, err
	}
	results := make([]ExperimentResult, 0, len(scenarios))
	for _, scenario := range scenarios {
		result, err := runScenario(ctx, experimentID, scenario)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func runScenario(
	ctx context.Context,
	experimentID string,
	scenario Scenario,
) (ExperimentResult, error) {
	if strings.TrimSpace(scenario.Name) == "" {
		return ExperimentResult{}, errors.New("run scenario: name cannot be empty")
	}
	if err := scenario.Config.Validate(); err != nil {
		return ExperimentResult{}, fmt.Errorf("run scenario %s: %w", scenario.Name, err)
	}

	plan, err := GenerateLatencyPlan(scenario.Config)
	if err != nil {
		return ExperimentResult{}, fmt.Errorf("run scenario %s: %w", scenario.Name, err)
	}
	baseline, err := NewBaselineOrchestrator(scenario.Config, plan)
	if err != nil {
		return ExperimentResult{}, fmt.Errorf("run scenario %s: %w", scenario.Name, err)
	}
	baselineRun, err := runArchitecture(
		ctx,
		ArchitectureBaseline,
		baseline,
		scenario.Config.RequestCount,
		scenario.Config.MaxClientConcurrency,
	)
	if err != nil {
		return ExperimentResult{}, fmt.Errorf("run scenario %s baseline: %w", scenario.Name, err)
	}
	baselineRun.Metrics = baseline.Metrics()
	if err := ensureArchitectureIdle(baseline, nil); err != nil {
		return ExperimentResult{}, fmt.Errorf("run scenario %s baseline: %w", scenario.Name, err)
	}

	proposal, err := NewProposalOrchestrator(scenario.Config, plan)
	if err != nil {
		return ExperimentResult{}, fmt.Errorf("run scenario %s: %w", scenario.Name, err)
	}
	if err := proposal.rescuePool.Reset(); err != nil {
		return ExperimentResult{}, fmt.Errorf("run scenario %s proposal reset: %w", scenario.Name, err)
	}
	proposalRun, err := runArchitecture(
		ctx,
		ArchitectureProposal,
		proposal,
		scenario.Config.RequestCount,
		scenario.Config.MaxClientConcurrency,
	)
	if err != nil {
		return ExperimentResult{}, fmt.Errorf("run scenario %s proposal: %w", scenario.Name, err)
	}
	proposalRun.Metrics = proposal.Metrics()
	proposalRun.RescuePool = proposal.RescuePoolSnapshot()
	if err := ensureArchitectureIdle(nil, proposal); err != nil {
		return ExperimentResult{}, fmt.Errorf("run scenario %s proposal: %w", scenario.Name, err)
	}

	return ExperimentResult{
		ExperimentID: experimentID,
		Scenario:     scenario,
		Baseline:     baselineRun,
		Proposal:     proposalRun,
	}, nil
}

func runArchitecture(
	ctx context.Context,
	architecture Architecture,
	executor clientRequestExecutor,
	requestCount int,
	maxConcurrency int,
) (ArchitectureExperiment, error) {
	startedAt := time.Now()
	executions, err := runClientRequests(ctx, executor, requestCount, maxConcurrency)
	finishedAt := time.Now()
	if err != nil {
		return ArchitectureExperiment{}, err
	}
	return ArchitectureExperiment{
		Architecture: architecture,
		StartedAt:    startedAt,
		FinishedAt:   finishedAt,
		WallElapsed:  finishedAt.Sub(startedAt),
		Executions:   executions,
	}, nil
}

func runClientRequests(
	ctx context.Context,
	executor clientRequestExecutor,
	requestCount int,
	maxConcurrency int,
) ([]RequestExecution, error) {
	if requestCount <= 0 {
		return nil, fmt.Errorf("request count must be greater than zero: %d", requestCount)
	}
	if maxConcurrency <= 0 {
		return nil, fmt.Errorf("max concurrency must be greater than zero: %d", maxConcurrency)
	}

	executions := make([]RequestExecution, requestCount)
	errorsByRequest := make([]error, requestCount)
	clientSlots := make(chan struct{}, maxConcurrency)
	var waitGroup sync.WaitGroup
	waitGroup.Add(requestCount)
	for requestID := 1; requestID <= requestCount; requestID++ {
		go func() {
			defer waitGroup.Done()
			select {
			case clientSlots <- struct{}{}:
				defer func() { <-clientSlots }()
			case <-ctx.Done():
				errorsByRequest[requestID-1] = ctx.Err()
				return
			}
			executions[requestID-1], errorsByRequest[requestID-1] = executor.Execute(
				ctx,
				ClientRequest{ID: requestID},
			)
		}()
	}
	waitGroup.Wait()

	for requestIndex, err := range errorsByRequest {
		if err != nil {
			return nil, fmt.Errorf("request %d: %w", requestIndex+1, err)
		}
	}
	return executions, nil
}

func ensureArchitectureIdle(
	baseline *BaselineOrchestrator,
	proposal *ProposalOrchestrator,
) error {
	if baseline != nil {
		for _, serviceID := range serviceIDs {
			current, _, _ := baseline.PrimaryOccupancy(serviceID)
			if current != 0 {
				return fmt.Errorf("primary %s still has occupancy %d", serviceID, current)
			}
		}
	}
	if proposal != nil {
		for _, serviceID := range serviceIDs {
			current, _, _ := proposal.PrimaryOccupancy(serviceID)
			if current != 0 {
				return fmt.Errorf("primary %s still has occupancy %d", serviceID, current)
			}
		}
		if current := proposal.RescuePoolSnapshot().Occupancy; current != 0 {
			return fmt.Errorf("rescue pool still has occupancy %d", current)
		}
	}
	return nil
}

func newExperimentID(now time.Time) (string, error) {
	location, err := time.LoadLocation("America/Lima")
	if err != nil {
		return "", fmt.Errorf("load America/Lima timezone: %w", err)
	}
	return now.In(location).Format("20060102T150405.000000000-0700"), nil
}

type csvFile struct {
	name   string
	header []string
	rows   [][]string
}

// ExportCSV writes all four outputs only after every temporary file has been
// completed successfully. Existing files are rejected unless overwrite=true.
func ExportCSV(results []ExperimentResult, outputDirectory string, overwrite bool) error {
	if len(results) == 0 {
		return errors.New("export CSV: no experiment results")
	}
	if strings.TrimSpace(outputDirectory) == "" {
		return errors.New("export CSV: output directory cannot be empty")
	}
	location, err := time.LoadLocation("America/Lima")
	if err != nil {
		return fmt.Errorf("export CSV: load America/Lima timezone: %w", err)
	}
	files := buildCSVFiles(results, location)
	if err := os.MkdirAll(outputDirectory, 0o755); err != nil {
		return fmt.Errorf("export CSV: create output directory: %w", err)
	}
	for _, file := range files {
		target := filepath.Join(outputDirectory, file.name)
		if _, err := os.Stat(target); err == nil && !overwrite {
			return fmt.Errorf("export CSV: %s already exists; use overwrite to replace it", target)
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("export CSV: inspect %s: %w", target, err)
		}
	}

	temporaryPaths := make([]string, 0, len(files))
	cleanup := func() {
		for _, temporaryPath := range temporaryPaths {
			_ = os.Remove(temporaryPath)
		}
	}
	for _, file := range files {
		temporary, err := os.CreateTemp(outputDirectory, "."+file.name+"-*")
		if err != nil {
			cleanup()
			return fmt.Errorf("export CSV: create temporary %s: %w", file.name, err)
		}
		temporaryPaths = append(temporaryPaths, temporary.Name())
		writer := csv.NewWriter(temporary)
		if err := writer.Write(file.header); err == nil {
			err = writer.WriteAll(file.rows)
		}
		writer.Flush()
		if err == nil {
			err = writer.Error()
		}
		if closeErr := temporary.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			cleanup()
			return fmt.Errorf("export CSV: write %s: %w", file.name, err)
		}
	}

	for index, file := range files {
		target := filepath.Join(outputDirectory, file.name)
		if err := os.Rename(temporaryPaths[index], target); err != nil {
			cleanup()
			return fmt.Errorf("export CSV: publish %s: %w", file.name, err)
		}
		temporaryPaths[index] = ""
	}
	return nil
}

func buildCSVFiles(results []ExperimentResult, location *time.Location) []csvFile {
	requestHeader := []string{
		"experiment_id", "scenario", "architecture", "request_id", "received_at_peru",
		"finished_at_peru", "elapsed_ns", "status", "results_count", "timeout_ns",
		"planned_slow_primaries", "rescue_candidates", "rescues_activated",
		"rescues_skipped_no_capacity", "rescues_successful",
	}
	attemptHeader := []string{
		"experiment_id", "scenario", "architecture", "request_id", "service_id",
		"attempt_id", "attempt_type", "instance_id", "planned_latency_ns", "planned_slow",
		"dispatched_at_peru", "started_at_peru", "finished_at_peru", "queue_wait_ns",
		"execution_ns", "total_elapsed_ns", "status", "cancellation_reason", "winner",
	}
	summaryHeader := []string{
		"experiment_id", "scenario", "architecture", "started_at_peru", "finished_at_peru",
		"wall_elapsed_ns", "configured_requests", "max_client_concurrency", "slow_probability",
		"normal_latency_min_ns", "normal_latency_max_ns", "slow_latency_min_ns",
		"slow_latency_max_ns", "hedge_threshold_ns", "rescue_workers", "capacity_per_instance",
		"rescue_budget_per_request", "timeout_ns", "seed", "equivalent_instances",
		"client_requests_received", "client_requests_completed", "client_requests_timed_out",
		"p50_ns", "p95_ns", "p99_ns", "attempts_dispatched", "attempts_started",
		"attempts_completed", "attempts_cancelled_before_start", "attempts_cancelled_after_start",
		"rescue_candidates", "rescues_activated", "rescues_skipped_no_capacity",
		"rescues_successful", "rescue_success_pct", "rescue_peak_occupancy",
		"rescue_utilization_pct", "throughput_rps",
	}

	baselineRows := make([][]string, 0)
	proposalRows := make([][]string, 0)
	attemptRows := make([][]string, 0)
	summaryRows := make([][]string, 0, len(results)*2)
	for _, result := range results {
		baselineRows = append(baselineRows, requestRows(result, result.Baseline, location)...)
		proposalRows = append(proposalRows, requestRows(result, result.Proposal, location)...)
		attemptRows = append(attemptRows, attemptRowsForRun(result, result.Baseline, location)...)
		attemptRows = append(attemptRows, attemptRowsForRun(result, result.Proposal, location)...)
		summaryRows = append(summaryRows, summaryRow(result, result.Baseline, location))
		summaryRows = append(summaryRows, summaryRow(result, result.Proposal, location))
	}

	return []csvFile{
		{name: "baseline.csv", header: requestHeader, rows: baselineRows},
		{name: "proposal.csv", header: requestHeader, rows: proposalRows},
		{name: "attempts.csv", header: attemptHeader, rows: attemptRows},
		{name: "summary.csv", header: summaryHeader, rows: summaryRows},
	}
}

func requestRows(
	result ExperimentResult,
	run ArchitectureExperiment,
	location *time.Location,
) [][]string {
	rows := make([][]string, 0, len(run.Executions))
	for _, execution := range run.Executions {
		plannedSlowPrimaries := 0
		for _, attempt := range execution.Attempts {
			if attempt.Attempt.Type == AttemptTypePrimary && attempt.Attempt.PlannedSlow {
				plannedSlowPrimaries++
			}
		}
		rows = append(rows, []string{
			result.ExperimentID,
			result.Scenario.Name,
			string(run.Architecture),
			strconv.Itoa(execution.Response.RequestID),
			formatTimestamp(execution.Response.StartedAt, location),
			formatTimestamp(execution.Response.FinishedAt, location),
			formatDuration(execution.Response.Elapsed),
			string(execution.Response.Status),
			strconv.Itoa(len(execution.Response.Results)),
			formatDuration(result.Scenario.Config.Timeout),
			strconv.Itoa(plannedSlowPrimaries),
			strconv.Itoa(execution.Rescues.Candidates),
			strconv.Itoa(execution.Rescues.Activated),
			strconv.Itoa(execution.Rescues.SkippedNoCapacity),
			strconv.Itoa(execution.Rescues.Successful),
		})
	}
	return rows
}

func attemptRowsForRun(
	result ExperimentResult,
	run ArchitectureExperiment,
	location *time.Location,
) [][]string {
	rows := make([][]string, 0)
	for _, execution := range run.Executions {
		for _, record := range execution.Attempts {
			rows = append(rows, []string{
				result.ExperimentID,
				result.Scenario.Name,
				string(run.Architecture),
				strconv.Itoa(record.Attempt.RequestID),
				string(record.Attempt.ServiceID),
				record.Attempt.ID,
				string(record.Attempt.Type),
				record.Attempt.InstanceID,
				formatDuration(record.Attempt.PlannedLatency),
				strconv.FormatBool(record.Attempt.PlannedSlow),
				formatTimestamp(record.DispatchedAt, location),
				formatTimestamp(record.StartedAt, location),
				formatTimestamp(record.FinishedAt, location),
				formatDuration(record.QueueWait),
				formatDuration(record.ExecutionTime),
				formatDuration(record.TotalElapsed),
				string(record.Status),
				string(record.CancellationReason),
				strconv.FormatBool(record.Winner),
			})
		}
	}
	return rows
}

func summaryRow(
	result ExperimentResult,
	run ArchitectureExperiment,
	location *time.Location,
) []string {
	config := result.Scenario.Config
	metrics := run.Metrics
	rescueWorkers := 0
	rescueBudget := 0
	equivalentInstances := ServiceCount
	rescuePeak := 0
	rescueUtilization := 0.0
	if run.Architecture == ArchitectureProposal {
		rescueWorkers = config.RescueWorkers
		rescueBudget = config.RescueBudgetPerRequest
		equivalentInstances += config.RescueWorkers
		rescuePeak = run.RescuePool.PeakOccupancy
		rescueUtilization = run.RescuePool.UtilizationPercent
	}
	rescueSuccess := 0.0
	if metrics.RescuesActivated > 0 {
		rescueSuccess = float64(metrics.RescuesSuccessful) / float64(metrics.RescuesActivated) * 100
	}
	throughput := 0.0
	if run.WallElapsed > 0 {
		throughput = float64(metrics.ClientRequestsReceived) / run.WallElapsed.Seconds()
	}

	return []string{
		result.ExperimentID,
		result.Scenario.Name,
		string(run.Architecture),
		formatTimestamp(run.StartedAt, location),
		formatTimestamp(run.FinishedAt, location),
		formatDuration(run.WallElapsed),
		strconv.Itoa(config.RequestCount),
		strconv.Itoa(config.MaxClientConcurrency),
		formatFloat(config.SlowProbability),
		formatDuration(config.NormalLatencyMin),
		formatDuration(config.NormalLatencyMax),
		formatDuration(config.SlowLatencyMin),
		formatDuration(config.SlowLatencyMax),
		formatDuration(config.HedgeThreshold),
		strconv.Itoa(rescueWorkers),
		strconv.Itoa(config.CapacityPerInstance),
		strconv.Itoa(rescueBudget),
		formatDuration(config.Timeout),
		strconv.FormatInt(config.Seed, 10),
		strconv.Itoa(equivalentInstances),
		strconv.Itoa(metrics.ClientRequestsReceived),
		strconv.Itoa(metrics.ClientRequestsCompleted),
		strconv.Itoa(metrics.ClientRequestsTimedOut),
		formatOptionalDuration(metrics.P50, metrics.HasPercentiles),
		formatOptionalDuration(metrics.P95, metrics.HasPercentiles),
		formatOptionalDuration(metrics.P99, metrics.HasPercentiles),
		strconv.Itoa(metrics.AttemptsDispatched),
		strconv.Itoa(metrics.AttemptsStarted),
		strconv.Itoa(metrics.AttemptsCompleted),
		strconv.Itoa(metrics.AttemptsCancelledBeforeStart),
		strconv.Itoa(metrics.AttemptsCancelledAfterStart),
		strconv.Itoa(metrics.RescueCandidates),
		strconv.Itoa(metrics.RescuesActivated),
		strconv.Itoa(metrics.RescuesSkippedNoCapacity),
		strconv.Itoa(metrics.RescuesSuccessful),
		formatFloat(rescueSuccess),
		strconv.Itoa(rescuePeak),
		formatFloat(rescueUtilization),
		formatFloat(throughput),
	}
}

func formatTimestamp(value time.Time, location *time.Location) string {
	if value.IsZero() {
		return ""
	}
	return value.In(location).Format(time.RFC3339Nano)
}

func formatDuration(value time.Duration) string {
	return strconv.FormatInt(value.Nanoseconds(), 10)
}

func formatOptionalDuration(value time.Duration, present bool) string {
	if !present {
		return ""
	}
	return formatDuration(value)
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}
