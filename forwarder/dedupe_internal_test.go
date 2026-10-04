package forwarder

import (
	"net/http"
	"testing"
	"time"

	"github.com/highcumontoa/service-discovery-lb-go/model"
)

// 业务：超过容量上限按 LRU 安全淘汰，被淘汰键再来正常进入缓存。
func TestDedupeStoreLRU(t *testing.T) {
	store := NewDedupeStoreWith(DedupeConfig{
		SuccessTTL: time.Minute, FailureTTL: time.Minute, Capacity: 2,
	})
	store.complete("a", "ins", http.StatusOK)
	store.complete("b", "ins", http.StatusOK)
	store.complete("c", "ins", http.StatusOK) // a 最久未访问 -> 淘汰

	if store.Len() != 2 {
		t.Fatalf("容量上限 2, got len=%d", store.Len())
	}
	if _, _, ok := store.Lookup("a"); ok {
		t.Fatal("a 应已被 LRU 淘汰")
	}
	if _, _, ok := store.Lookup("b"); !ok {
		t.Fatal("b 应仍保留")
	}
	// 访问 b 后再插入 d：此时最久未访问的是 c。
	_, _, _ = store.Lookup("b")
	store.complete("d", "ins", http.StatusOK)
	if _, _, ok := store.Lookup("c"); ok {
		t.Fatal("访问 b 后，c 应为最久未访问并被淘汰")
	}
	if _, _, ok := store.Lookup("b"); !ok {
		t.Fatal("b 刚被访问应保留")
	}
	if _, _, ok := store.Lookup("d"); !ok {
		t.Fatal("d 为新插入应保留")
	}
	// 被淘汰键重新发布后重新进入缓存，不报错、可正常复用。
	store.complete("a", "ins", http.StatusOK)
	if _, _, ok := store.Lookup("a"); !ok {
		t.Fatal("淘汰后的键重新发布后应可正常复用")
	}
	store.Len()
	t.Logf("业务=容量 LRU：容量 2 下淘汰最久未访问；命中刷新位置；淘汰后重新发布正常, len=%d", store.Len())
}

// 业务：失败与成功使用各自保留窗口；失败条目不会被 Lookup 当成功返回。
func TestDedupeStoreFailureAndSuccessTTL(t *testing.T) {
	clock := time.Unix(1000, 0)
	store := NewDedupeStoreWith(DedupeConfig{
		SuccessTTL: 100 * time.Millisecond, FailureTTL: 20 * time.Millisecond,
		Capacity: 8, Now: func() time.Time { return clock },
	})
	store.complete("ok", "ins", http.StatusOK)
	store.completeFailure("boom", model.ErrUpstream4xx, model.ErrUpstream4xx, "ins", "后端返回 404")

	// 初始时刻两者都在。
	if store.Len() != 2 {
		t.Fatalf("应缓存 2 条结论, got %d", store.Len())
	}
	// 失败条目 Lookup 不返回成功，但 lookupOutcome 仍可读到失败结论。
	if _, _, ok := store.Lookup("boom"); ok {
		t.Fatal("失败结论不得被 Lookup 当作成功")
	}
	if o, ok := store.lookupOutcome("boom"); !ok || o.Kind != OutcomeFailure || o.ErrKind != model.ErrUpstream4xx {
		t.Fatalf("失败结论应可内部读取且保留类别, got %+v ok=%v", o, ok)
	}

	// 30ms 后：失败结论过期，成功结论仍在。
	clock = clock.Add(30 * time.Millisecond)
	if o, ok := store.lookupOutcome("boom"); ok {
		t.Fatalf("失败结论应已过期, got %+v", o)
	}
	if _, _, ok := store.Lookup("ok"); !ok {
		t.Fatal("成功结论在其更长窗口内应仍可复用")
	}
	// 过期条目在下次 acquire 时被惰性清理。
	leader, _ := store.acquire("boom")
	if !leader {
		t.Fatal("失败结论过期后同键再来应成为全新 leader")
	}
	// 150ms 后成功结论也过期。
	clock = clock.Add(120 * time.Millisecond)
	if _, _, ok := store.Lookup("ok"); ok {
		t.Fatal("成功结论超过窗口后必须过期淘汰")
	}
	if store.Len() != 0 {
		t.Fatalf("全部过期后缓存应为空, len=%d", store.Len())
	}
	t.Logf("业务=差异化保留窗口: 失败 20ms / 成功 100ms 分别过期；失败不伪装成功；过期后同键成为全新请求")
}
