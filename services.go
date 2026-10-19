package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// ServiceID identifies one of the ten logical, independent services.
type ServiceID string

const (
	UserService           ServiceID = "UserService"
	ProductService        ServiceID = "ProductService"
	InventoryService      ServiceID = "InventoryService"
	PricingService        ServiceID = "PricingService"
	PromotionService      ServiceID = "PromotionService"
	TaxService            ServiceID = "TaxService"
	ShippingService       ServiceID = "ShippingService"
	PaymentService        ServiceID = "PaymentService"
	RiskService           ServiceID = "RiskService"
	RecommendationService ServiceID = "RecommendationService"
)

var serviceIDs = [...]ServiceID{
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

// ServiceIDs returns the complete service list in a stable order. Returning a
// copy prevents callers from changing the canonical order.
func ServiceIDs() []ServiceID {
	ids := make([]ServiceID, len(serviceIDs))
	copy(ids, serviceIDs[:])
	return ids
}

type ServiceResponseStatus string

const ServiceResponseOK ServiceResponseStatus = "ok"

// ServiceResponse is the structured result returned by a logical service.
type ServiceResponse struct {
	RequestID int
	ServiceID ServiceID
	Status    ServiceResponseStatus
	Data      string
}

// LogicalService is one deterministic, side-effect-free service handler.
// Primary and rescue attempts use the same value, so attempt placement cannot
// change the business response.
type LogicalService struct {
	ID ServiceID
}

// Services returns the ten logical handlers in their stable canonical order.
func Services() []LogicalService {
	services := make([]LogicalService, 0, len(serviceIDs))
	for _, id := range serviceIDs {
		services = append(services, LogicalService{ID: id})
	}
	return services
}

// Handle waits for the planned artificial latency and then returns a
// deterministic response. The wait observes ctx, allowing a losing or timed
// out attempt to stop promptly.
func (s LogicalService) Handle(
	ctx context.Context,
	requestID int,
	plannedLatency time.Duration,
) (ServiceResponse, error) {
	if err := waitForLatency(ctx, plannedLatency); err != nil {
		return ServiceResponse{}, err
	}

	return ServiceResponse{
		RequestID: requestID,
		ServiceID: s.ID,
		Status:    ServiceResponseOK,
		Data:      fmt.Sprintf("request-%d:%s-data", requestID, s.ID),
	}, nil
}

func waitForLatency(ctx context.Context, latency time.Duration) error {
	if latency < 0 {
		return fmt.Errorf("planned latency cannot be negative: %s", latency)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if latency == 0 {
		return nil
	}

	timer := time.NewTimer(latency)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// LatencySample is one pre-generated latency and the distribution branch from
// which it came. Slow is stored explicitly rather than inferred later.
type LatencySample struct {
	Duration time.Duration
	Slow     bool
}

// ServiceLatencies contains both possible attempts for one request/service
// pair. Rescue is generated even if the proposal never activates it.
type ServiceLatencies struct {
	Primary LatencySample
	Rescue  LatencySample
}

type latencyPlanKey struct {
	requestID int
	serviceID ServiceID
}

// LatencyPlan is the immutable workload shared by baseline and proposal. Its
// internal map is not exposed, which prevents either architecture from
// changing the planned values.
type LatencyPlan struct {
	requestCount int
	entries      map[latencyPlanKey]ServiceLatencies
}

// GenerateLatencyPlan samples every primary and potential rescue latency
// before concurrent execution begins. Separate deterministic PRNG streams keep
// primary and rescue sampling independent from each other and from goroutine
// scheduling.
func GenerateLatencyPlan(config Config) (LatencyPlan, error) {
	if err := config.Validate(); err != nil {
		return LatencyPlan{}, fmt.Errorf("generate latency plan: %w", err)
	}

	primaryRandom := rand.New(rand.NewSource(deriveRandomSeed(config.Seed, 0)))
	rescueRandom := rand.New(rand.NewSource(deriveRandomSeed(config.Seed, 1)))
	entries := make(map[latencyPlanKey]ServiceLatencies, config.RequestCount*ServiceCount)

	for requestID := 1; requestID <= config.RequestCount; requestID++ {
		for _, serviceID := range serviceIDs {
			entries[latencyPlanKey{requestID: requestID, serviceID: serviceID}] = ServiceLatencies{
				Primary: sampleLatency(primaryRandom, config),
				Rescue:  sampleLatency(rescueRandom, config),
			}
		}
	}

	return LatencyPlan{
		requestCount: config.RequestCount,
		entries:      entries,
	}, nil
}

// RequestCount returns the number of requests represented by the plan.
func (p LatencyPlan) RequestCount() int {
	return p.requestCount
}

// Latencies returns a copy of the planned values for one logical operation.
func (p LatencyPlan) Latencies(requestID int, serviceID ServiceID) (ServiceLatencies, bool) {
	latencies, ok := p.entries[latencyPlanKey{requestID: requestID, serviceID: serviceID}]
	return latencies, ok
}

func sampleLatency(random *rand.Rand, config Config) LatencySample {
	slow := random.Float64() < config.SlowProbability
	if slow {
		return LatencySample{
			Duration: randomDuration(random, config.SlowLatencyMin, config.SlowLatencyMax),
			Slow:     true,
		}
	}

	return LatencySample{
		Duration: randomDuration(random, config.NormalLatencyMin, config.NormalLatencyMax),
		Slow:     false,
	}
}

func randomDuration(random *rand.Rand, minDuration, maxDuration time.Duration) time.Duration {
	if minDuration == maxDuration {
		return minDuration
	}

	span := maxDuration - minDuration
	const maxInt64 = int64(^uint64(0) >> 1)
	if int64(span) == maxInt64 {
		return minDuration + time.Duration(random.Uint64()&uint64(maxInt64))
	}

	return minDuration + time.Duration(random.Int63n(int64(span)+1))
}

// deriveRandomSeed uses the SplitMix64 finalizer to create stable, distinct
// seeds for independent pseudo-random streams. It is not used for security.
func deriveRandomSeed(seed int64, stream uint64) int64 {
	value := uint64(seed) + 0x9e3779b97f4a7c15*(stream+1)
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return int64(value ^ (value >> 31))
}
