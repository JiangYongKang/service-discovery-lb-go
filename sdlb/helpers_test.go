package sdlb

import (
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/health"
)

func healthCfg(interval, timeout time.Duration) health.Config {
	return health.Config{
		Interval:         interval,
		Timeout:          timeout,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		ProbeInterval:    interval / 2,
	}
}

func waitHealthy(t *testing.T, s *SDLB, service string, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(s.Registry().HealthyInstances(service)) == n {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timeout waiting %d healthy instances for %s\n%s",
		n, service, s.ExplainStatus(service))
}
