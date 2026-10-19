package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type CLIOptions struct {
	Scenarios       []Scenario
	OutputDirectory string
	Overwrite       bool
}

func main() {
	options, err := parseCLIOptions(os.Args[1:], os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	results, err := RunExperiments(ctx, options.Scenarios)
	if err != nil {
		fmt.Fprintf(os.Stderr, "experiment failed: %v\n", err)
		os.Exit(1)
	}
	if err := ExportCSV(results, options.OutputDirectory, options.Overwrite); err != nil {
		fmt.Fprintf(os.Stderr, "export failed: %v\n", err)
		os.Exit(1)
	}

	printExperimentSummary(results, options.OutputDirectory)
}

func parseCLIOptions(args []string, output io.Writer) (CLIOptions, error) {
	defaults := DefaultConfig()
	flags := flag.NewFlagSet("tail-latency-poc", flag.ContinueOnError)
	flags.SetOutput(output)
	scenarioName := flags.String("scenario", "functional", "functional, main, load, extended, custom, or all")
	requestCount := flags.Int("requests", 0, "override request count (0 uses scenario preset)")
	maxConcurrency := flags.Int("concurrency", 0, "override maximum client concurrency (0 uses scenario preset)")
	slowProbability := flags.Float64("slow-probability", defaults.SlowProbability, "probability of a slow latency in [0,1]")
	normalMin := flags.Duration("normal-min", defaults.NormalLatencyMin, "minimum normal latency")
	normalMax := flags.Duration("normal-max", defaults.NormalLatencyMax, "maximum normal latency")
	slowMin := flags.Duration("slow-min", defaults.SlowLatencyMin, "minimum slow latency")
	slowMax := flags.Duration("slow-max", defaults.SlowLatencyMax, "maximum slow latency")
	hedgeThreshold := flags.Duration("hedge-threshold", defaults.HedgeThreshold, "delay before a pending service becomes a rescue candidate")
	rescueWorkers := flags.Int("rescue-workers", defaults.RescueWorkers, "number of shared rescue workers")
	capacity := flags.Int("capacity", defaults.CapacityPerInstance, "concurrent operations per instance")
	rescueBudget := flags.Int("rescue-budget", defaults.RescueBudgetPerRequest, "maximum rescues activated per client request")
	maxRescuesPerService := flags.Int("max-rescues-per-service", defaults.MaxRescueAttemptsPerService, "maximum rescues for one service in a request")
	timeout := flags.Duration("timeout", defaults.Timeout, "global client request timeout")
	seed := flags.Int64("seed", defaults.Seed, "pseudo-random seed")
	verbose := flags.Bool("verbose", false, "print primary and rescue lifecycle events")
	logRequestID := flags.Int("log-request", 0, "only log this request ID (0 logs every request)")
	outputDirectory := flags.String("output", "results", "directory for the four CSV files")
	overwrite := flags.Bool("overwrite", false, "replace existing CSV files")
	if err := flags.Parse(args); err != nil {
		return CLIOptions{}, err
	}
	if flags.NArg() != 0 {
		return CLIOptions{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}

	base := defaults
	base.SlowProbability = *slowProbability
	base.NormalLatencyMin = *normalMin
	base.NormalLatencyMax = *normalMax
	base.SlowLatencyMin = *slowMin
	base.SlowLatencyMax = *slowMax
	base.HedgeThreshold = *hedgeThreshold
	base.RescueWorkers = *rescueWorkers
	base.CapacityPerInstance = *capacity
	base.RescueBudgetPerRequest = *rescueBudget
	base.MaxRescueAttemptsPerService = *maxRescuesPerService
	base.Timeout = *timeout
	base.Seed = *seed
	base.Verbose = *verbose
	base.LogRequestID = *logRequestID
	scenarios, err := PresetScenarios(*scenarioName, base)
	if err != nil {
		return CLIOptions{}, err
	}
	for index := range scenarios {
		if *requestCount != 0 {
			scenarios[index].Config.RequestCount = *requestCount
		}
		if *maxConcurrency != 0 {
			scenarios[index].Config.MaxClientConcurrency = *maxConcurrency
		}
		if err := scenarios[index].Config.Validate(); err != nil {
			return CLIOptions{}, fmt.Errorf("scenario %s: %w", scenarios[index].Name, err)
		}
	}

	return CLIOptions{
		Scenarios:       scenarios,
		OutputDirectory: *outputDirectory,
		Overwrite:       *overwrite,
	}, nil
}

func printExperimentSummary(results []ExperimentResult, outputDirectory string) {
	fmt.Printf("%-12s %-10s %12s %12s %12s %12s\n", "scenario", "architecture", "p50", "p95", "p99", "attempts")
	for _, result := range results {
		for _, run := range []ArchitectureExperiment{result.Baseline, result.Proposal} {
			fmt.Printf(
				"%-12s %-10s %12s %12s %12s %12d\n",
				result.Scenario.Name,
				run.Architecture,
				formatConsoleDuration(run.Metrics.P50, run.Metrics.HasPercentiles),
				formatConsoleDuration(run.Metrics.P95, run.Metrics.HasPercentiles),
				formatConsoleDuration(run.Metrics.P99, run.Metrics.HasPercentiles),
				run.Metrics.AttemptsStarted,
			)
		}
	}
	fmt.Printf("CSV files written to %s\n", outputDirectory)
}

func formatConsoleDuration(value time.Duration, present bool) string {
	if !present {
		return "n/a"
	}
	return value.String()
}
