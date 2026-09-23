// Command demo 演示本地服务发现、健康摘除、重试降级与规则热更新的完整链路。
// 运行: go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/backend"
	"github.com/highcumontoa/service-discovery-lb-go/health"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/mesh"
	"github.com/highcumontoa/service-discovery-lb-go/model"
	"github.com/highcumontoa/service-discovery-lb-go/router"
)

func main() {
	logger := log.New(log.Writer(), "", log.Ltime)
	logf := func(format string, args ...any) { logger.Printf("    "+format, args...) }

	// 1) 装配：2 次失败摘除，2 次成功恢复。
	probe := health.NewHTTPProbe(300 * time.Millisecond).Func()
	m, err := mesh.New(mesh.Config{
		FailThreshold:    2,
		RecoverThreshold: 2,
		Probe:            probe,
		ProbeInterval:    200 * time.Millisecond,
		InitialRules: router.Rules{Version: 1, ByService: map[string]router.Rule{
			"echo": {
				Service: "echo", MaxAttempts: 3,
				PerCallTimeout: 400 * time.Millisecond,
				RetryableKinds: map[model.ErrorKind]bool{
					model.ErrConnectFailed: true,
					model.ErrTimeout:       true,
					model.ErrUpstream5xx:   true,
				},
			},
		}},
		Logf: logf,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 2) 启动 3 个本地后端进程并注册。
	var bes []*backend.Instance
	instByID := map[string]model.Instance{}
	for i := 1; i <= 3; i++ {
		be, err := backend.New(fmt.Sprintf("inst-%d", i), "echo", backend.Behavior{})
		if err != nil {
			log.Fatal(err)
		}
		bes = append(bes, be)
		_ = m.Register(be.ToModel())
		instByID[be.ID] = be.ToModel()
	}
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()

	call := func(key string) {
		res, err := m.Call(context.Background(), model.RequestContext{
			Service: "echo", Method: "POST", Path: "/job", IdempotencyKey: key,
		})
		if err != nil {
			logger.Printf("CALL key=%s 失败 kind=%s: %v", key, model.ErrKind(err), err)
			return
		}
		logger.Printf("CALL key=%s 成功 served=%s status=%d dedupe=%v",
			key, res.InstanceID, res.StatusCode, res.DedupeHit)
	}

	// 3) 探测就绪。
	for r := 0; r < 2; r++ {
		m.Monitor.RunOnceWith(instByID)
	}
	call("k1")

	// 4) 注入故障：关闭 inst-1（连接拒绝），转发应重试到其他实例。
	logger.Printf("== 注入故障：关闭 inst-1 ==")
	bes[0].Close()
	call("k2")

	// 5) 让探测把 inst-1 摘除（注册表中仍在册但不健康）。
	//    先把它从探测集合保留（地址仍指向旧端口），跑 2 轮失败。
	m.Monitor.RunOnceWith(instByID)
	m.Monitor.RunOnceWith(instByID)
	logger.Printf("== inst-1 健康状态: %s ==", m.Checker.GetStatus("inst-1").State)

	// 6) 规则热更新：切换 least_conn 策略；非法版本被拒绝。
	logger.Printf("== 热更新为 least_conn ==")
	_ = m.UpdateRules(router.Rules{Version: 2, ByService: map[string]router.Rule{
		"echo": {
			Service: "echo", Policy: lb.PolicyLeastConn,
			MaxAttempts: 2, PerCallTimeout: 400 * time.Millisecond,
		},
	}})
	bad := router.Rules{Version: 3, ByService: map[string]router.Rule{
		"echo": {Service: "echo", Policy: "bogus"},
	}}
	if err := m.UpdateRules(bad); err != nil {
		logger.Printf("非法热更新被拒绝并保留旧版: kind=%s", model.ErrKind(err))
	}
	call("k3")
}
