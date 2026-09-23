// 演示：本地进程实例模拟后端，展示注册、探活摘除、安全重试、
// fail-open 降级、一致哈希粘性与规则热更新。
//
//	go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/backend"
	"github.com/highcumontoa/service-discovery-lb-go/discovery"
	"github.com/highcumontoa/service-discovery-lb-go/forward"
	"github.com/highcumontoa/service-discovery-lb-go/health"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/route"
	"github.com/highcumontoa/service-discovery-lb-go/sdlb"
)

func main() {
	logger := log.New(os.Stdout, "[demo] ", log.Ltime|log.Lmsgprefix)

	cfg := sdlb.Config{Health: health.Config{
		Interval: 300 * time.Millisecond, Timeout: 200 * time.Millisecond,
		FailureThreshold: 2, SuccessThreshold: 2, ProbeInterval: 150 * time.Millisecond,
	}}
	sys := sdlb.New(cfg, backend.TCPProbe())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys.Start(ctx)

	const svc = "echo"
	// 启动 3 个真实 TCP 后端
	var srvs []*backend.Server
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("echo-%d", i)
		srv, err := backend.New(id, svc, true)
		if err != nil {
			log.Fatal(err)
		}
		defer srv.Stop()
		if err := sys.RegisterInstance(id, svc, srv.Addr, nil); err != nil {
			log.Fatal(err)
		}
		srvs = append(srvs, srv)
	}
	logger.Println("waiting for probes to mark instances healthy...")
	waitFor(func() bool { return len(sys.Registry().HealthyInstances(svc)) == 3 }, time.Second*3)
	fmt.Println(sys.ExplainStatus(svc))

	// 初始规则：一致哈希、不重试
	ver, err := sys.SetRules([]*route.Rule{{
		Service: svc, Strategy: lb.StrategyConsistent,
		Timeout: 600 * time.Millisecond, MaxAttempts: 1,
	}})
	must(err)
	logger.Printf("rules published v%d: strategy=%s", ver, lb.StrategyConsistent)

	dial := backend.DialInvoke()
	invoke := func(ctx context.Context, inst *discovery.Instance) (any, error) {
		resp, err := dial(ctx, inst.Addr, inst.ID)
		if err != nil {
			if strings.Contains(err.Error(), "timeout") || ctx.Err() == context.DeadlineExceeded {
				return nil, forward.ErrUpstreamTimeout
			}
			return nil, fmt.Errorf("%w: %v", forward.ErrConnectionRefused, err)
		}
		return resp, nil
	}

	call := func(key string) {
		res, err := sys.Call(context.Background(), svc, key, invoke)
		if err != nil {
			logger.Printf("call key=%s ruleVer=%d FAILED: %v\n%s", key, res.RuleVer, err, res.Explain())
			return
		}
		logger.Printf("call key=%-5s ruleVer=%d outcome=%s resp=%v",
			key, res.RuleVer, res.Outcome, res.Response)
	}

	logger.Println("--- consistent-hash stickiness: user-a always lands on the same node ---")
	for i := 0; i < 4; i++ {
		call("user-a")
	}

	logger.Println("--- switch to weighted (echo-0 weight 5) with retries + fail-open ---")
	ver, err = sys.SetRules([]*route.Rule{{
		Service: svc, Strategy: lb.StrategyWeighted,
		Weights: map[string]int{"echo-0": 5, "echo-1": 1, "echo-2": 1},
		Timeout: 600 * time.Millisecond, MaxAttempts: 3, FailOpen: true,
	}})
	must(err)
	for i := 0; i < 7; i++ {
		call("")
	}

	logger.Println("--- kill echo-0 process: dial refused -> safe retry on another node ---")
	srvs[0].Stop()
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 3; i++ {
		call("")
	}
	time.Sleep(300 * time.Millisecond)
	fmt.Println(sys.ExplainStatus(svc))

	logger.Println("--- stall echo-1: probe failure removes it; traffic stays on echo-2 ---")
	srvs[1].SetHealthy(false)
	srvs[1].Stall(2 * time.Second)
	time.Sleep(900 * time.Millisecond)
	fmt.Println(sys.ExplainStatus(svc))
	for i := 0; i < 3; i++ {
		call("")
	}

	logger.Println("--- all backends unavailable -> deterministic fail-open ---")
	srvs[2].Stop()
	time.Sleep(200 * time.Millisecond)
	call("")

	logger.Println("--- invalid hot update is rejected and previous rules stay effective ---")
	if _, err := sys.SetRules([]*route.Rule{{Service: ""}}); err != nil {
		logger.Printf("invalid update rejected as expected: %v", err)
	}
	logger.Printf("current effective rule version remains v%d", sys.Rules().CurrentVersion())
}

func waitFor(cond func() bool, within time.Duration) {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
