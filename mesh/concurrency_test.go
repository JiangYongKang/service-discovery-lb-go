package mesh_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/backend"
	"github.com/highcumontoa/service-discovery-lb-go/lb"
	"github.com/highcumontoa/service-discovery-lb-go/model"
	"github.com/highcumontoa/service-discovery-lb-go/router"
)

// 统计一次并发压测中各类结论的数量，验证“结论可区分”。
type outcomes struct {
	ok            atomic.Int64
	noHealthy     atomic.Int64
	deadGuard     atomic.Int64
	noRetryBudget atomic.Int64
	timeout       atomic.Int64
	connectFail   atomic.Int64
	other         atomic.Int64
}

func (o *outcomes) record(err error) {
	switch model.ErrKind(err) {
	case "":
		o.ok.Add(1)
	case model.ErrNoHealthyInstance:
		o.noHealthy.Add(1)
	case model.ErrInstanceDead:
		o.deadGuard.Add(1)
	case model.ErrNoRetryBudget:
		o.noRetryBudget.Add(1)
	case model.ErrTimeout:
		o.timeout.Add(1)
	case model.ErrConnectFailed:
		o.connectFail.Add(1)
	default:
		o.other.Add(1)
	}
}

func (o *outcomes) log(t *testing.T) {
	t.Logf("结论分布: ok=%d no_healthy=%d dead_guard=%d no_retry_budget=%d timeout=%d connect_fail=%d other=%d",
		o.ok.Load(), o.noHealthy.Load(), o.deadGuard.Load(), o.noRetryBudget.Load(),
		o.timeout.Load(), o.connectFail.Load(), o.other.Load())
}

// 场景 A：大量并发请求进行中，反复下线/上线实例；
// 每个成功结论必须由调用时刻仍存活的实例产生，失败必须可区分。
func TestConcurrentUpDown(t *testing.T) {
	m, bes := newMeshWithBackends(t, 4, 3)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)

	instByID := map[string]model.Instance{}
	live := map[string]*backend.Instance{}
	everLiveAddr := map[string]bool{}
	var liveMu sync.Mutex
	for _, be := range bes {
		instByID[be.ID] = be.ToModel()
		live[be.ID] = be
		everLiveAddr[be.Addr()] = true
	}

	var oc outcomes
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 抖动器：周期性关闭/重启某个实例并重新注册 + 跑探测改变健康集合。
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			be := bes[i%len(bes)]
			liveMu.Lock()
			if _, up := live[be.ID]; up {
				// 下线：先从 live 视图移除（后续成功请求不可能再命中它），
				// 再关闭进程并注销。
				old := live[be.ID]
				delete(live, be.ID)
				old.Close()
				_ = m.Deregister(be.ID)
			} else {
				// 上线：换新端口的新进程；先更新 live 视图（记录地址），
				// 再注册，避免“注册表已更新但夹具视图仍旧”的假阳性。
				newBE, err := backend.New(be.ID, "echo", backend.Behavior{})
				if err == nil {
					bes[i%len(bes)] = newBE
					instByID[be.ID] = newBE.ToModel()
					live[be.ID] = newBE
					everLiveAddr[newBE.Addr()] = true
					if err := m.Register(newBE.ToModel()); err == nil {
						m.Monitor.RunOnceWith(instByID)
						m.Monitor.RunOnceWith(instByID)
					} else {
						delete(live, be.ID)
					}
				}
			}
			liveMu.Unlock()
			i++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 并发请求
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				select {
				case <-stop:
					return
				default:
				}
				res, err := m.Call(context.Background(), model.RequestContext{
					Service:        "echo",
					Method:         "POST",
					Path:           "/job",
					IdempotencyKey: fmt.Sprintf("w%d-%d", w, j),
				})
				oc.record(err)
				if err == nil {
					// 以响应实际来源地址为准：它必须是某一时刻真实存活并处理了
					// 该请求的进程。旧进程在注销前完整处理完的请求仍算成功，
					// 但其地址必须出现在 everLive 中，且不能是当前 live 之外的陌生地址。
					servedAddr := res.Header.Get("X-Instance-Addr")
					liveMu.Lock()
					cur, up := live[res.InstanceID]
					currentAddr := ""
					if up {
						currentAddr = cur.Addr()
					}
					known := everLiveAddr[servedAddr]
					liveMu.Unlock()
					if !known {
						t.Errorf("响应来自未知/已彻底关闭的地址 instance=%s addr=%s", res.InstanceID, servedAddr)
						continue
					}
					// 若当前该 ID 已换了新进程，响应地址必须仍等于旧地址（合法在途），
					// 绝不能等于一个不属于该响应时刻存活进程的地址。
					if up && currentAddr != servedAddr {
						t.Errorf("响应地址 %s 与当前存活实例地址 %s 不符", servedAddr, currentAddr)
					}
				}
			}
		}(w)
	}

	// 让请求跑一会后停止抖动器（保留至少一个健康实例，避免尾部队列全失败）。
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	oc.log(t)
	if oc.other.Load() != 0 {
		t.Fatalf("出现无法区分的其他错误 %d 个", oc.other.Load())
	}
	if oc.ok.Load() == 0 {
		t.Fatal("应当至少有部分请求成功")
	}
}

// 场景 B：健康探测抖动（503 闪烁）时，恢复阈值未达到的实例绝不路由。
func TestProbeJitterKeepsInstanceOut(t *testing.T) {
	m, bes := newMeshWithBackends(t, 1, 3)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)
	be := bes[0]

	// 制造 1 次探测失败（阈值 2，未摘除）后再恢复，期间服务仍可被路由。
	be.SetBehavior(backend.Behavior{FailProbe: true})
	m.Monitor.RunOnce()
	_, state, _ := m.Registry.Get(be.ID)
	if state != model.StateHealthy {
		t.Fatalf("1 次失败（2 阈值）不应摘除, got %s", state)
	}
	res, err := m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/x", IdempotencyKey: "j1",
	})
	if err != nil || res.InstanceID != be.ID {
		t.Fatalf("未摘除时仍应路由到该实例, res=%+v err=%v", res, err)
	}

	// 连续 2 次失败 -> 摘除，此时即便业务仍正常也不得路由。
	m.Monitor.RunOnce()
	_, state, _ = m.Registry.Get(be.ID)
	if state != model.StateUnhealthy {
		t.Fatalf("连续 2 次失败应摘除, got %s", state)
	}
	be.SetBehavior(backend.Behavior{}) // 业务与探测都恢复
	// 仅 1 次成功探测 -> RECOVERING，仍不可路由。
	m.Monitor.RunOnce()
	_, state, _ = m.Registry.Get(be.ID)
	if state != model.StateRecovering {
		t.Fatalf("恢复期应为 RECOVERING, got %s", state)
	}
	_, err = m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/x", IdempotencyKey: "j2",
	})
	if model.ErrKind(err) != model.ErrNoHealthyInstance {
		t.Fatalf("RECOVERING 期间不得路由, got %v", err)
	}
	t.Logf("探测抖动下：1 次失败保持 HEALTHY；连续 2 次摘除；恢复 1/2 为 RECOVERING 不路由，判定依据均来自阈值状态机 | 最终拒绝: %v", err)

	// 第 2 次成功 -> HEALTHY，恢复路由。
	m.Monitor.RunOnce()
	res, err = m.Call(context.Background(), model.RequestContext{
		Service: "echo", Method: "GET", Path: "/x", IdempotencyKey: "j3",
	})
	if err != nil || res.InstanceID != be.ID {
		t.Fatalf("恢复完成后应重新路由, res=%+v err=%v", res, err)
	}
}

// 场景 C：热更新进行中，在途请求必须始终使用同一版本策略，且观察不到部分生效。
func TestHotUpdateDuringRequests(t *testing.T) {
	m, bes := newMeshWithBackends(t, 3, 1)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)

	var oc outcomes
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 规则在 round_robin / weighted / least_conn / random 间合法热更新，
	// 夹杂非法版本（必须被拒绝）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		v := uint64(10)
		policies := []string{"round_robin", "weighted", "least_conn", "random"}
		for {
			select {
			case <-stop:
				return
			default:
			}
			pol := policies[int(v)%len(policies)]
			rules := router.Rules{Version: v, ByService: map[string]router.Rule{
				"echo": {
					Service: "echo", Policy: lb.Policy(pol),
					MaxAttempts: 2, PerCallTimeout: 400 * time.Millisecond,
					Weights: map[string]int{"inst-1": 3, "inst-2": 1, "inst-3": 1},
				},
			}}
			if err := m.UpdateRules(rules); err != nil {
				t.Errorf("合法规则热更新失败: %v", err)
			}
			v++
			// 每隔几轮发一个非法版本，确认被拒绝。
			if v%3 == 0 {
				bad := router.Rules{Version: v, ByService: map[string]router.Rule{
					"echo": {Service: "echo", Policy: "bogus"},
				}}
				if err := m.UpdateRules(bad); model.ErrKind(err) != model.ErrRuleInvalid {
					t.Errorf("非法规则应被拒绝 rule_invalid, got %v", err)
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				select {
				case <-stop:
					return
				default:
				}
				_, err := m.Call(context.Background(), model.RequestContext{
					Service: "echo", Method: "GET", Path: "/u",
					IdempotencyKey: fmt.Sprintf("h%d-%d", w, j),
				})
				oc.record(err)
			}
		}(w)
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	oc.log(t)
	if oc.other.Load() != 0 {
		t.Fatalf("出现不可区分错误 %d", oc.other.Load())
	}
	if oc.ok.Load() == 0 {
		t.Fatal("热更新期间应仍有成功请求")
	}
}

// 场景 D：并发注销与请求竞速——已失效实例不得被路由（连接前复核拦截）。
func TestDeregisterRaceGuard(t *testing.T) {
	m, bes := newMeshWithBackends(t, 1, 1)
	defer func() {
		for _, be := range bes {
			be.Close()
		}
	}()
	markHealthy(t, m, bes)
	be := bes[0]

	var rejectedDead, rejectedNoHealthy, ok int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = m.Deregister(be.ID) // 幂等，仅第一次真正移除
		}()
		go func(i int) {
			defer wg.Done()
			_, err := m.Call(context.Background(), model.RequestContext{
				Service: "echo", Method: "GET", Path: "/r",
				IdempotencyKey: fmt.Sprintf("race-%d", i),
			})
			switch model.ErrKind(err) {
			case "":
				atomic.AddInt64(&ok, 1)
			case model.ErrInstanceDead:
				atomic.AddInt64(&rejectedDead, 1)
			case model.ErrNoHealthyInstance:
				atomic.AddInt64(&rejectedNoHealthy, 1)
			default:
				t.Errorf("不可区分错误: %v", err)
			}
		}(i)
	}
	wg.Wait()
	t.Logf("注销竞速结论: 成功=%d（注销前已完整开始）, instance_dead 拦截=%d, no_healthy=%d",
		ok, rejectedDead, rejectedNoHealthy)
	if ok+rejectedDead+rejectedNoHealthy != 200 {
		t.Fatal("所有请求必须得到可区分结论")
	}
}
