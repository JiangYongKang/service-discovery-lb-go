package lb

import (
	"hash/fnv"
	"sort"
	"strconv"

	"github.com/highcumontoa/service-discovery-lb-go/discovery"
)

// consistentHash 一致性哈希：相同 HashKey 在相同实例集合下恒定路由到同一实例。
// 采用 FNV-1 + 每实例固定虚拟节点数；环由当前快照构建，热更新/上下线后自然重映射，
// 但在途请求持有自己的快照，因此路由结论不受后续变更影响。
type consistentHash struct {
	weights map[string]int
}

const virtualNodes = 32

func newConsistentHash(weights map[string]int) *consistentHash {
	w := make(map[string]int, len(weights))
	for k, v := range weights {
		if v > 0 {
			w[k] = v
		}
	}
	return &consistentHash{weights: w}
}

func (c *consistentHash) Name() Strategy { return StrategyConsistent }

type ringPoint struct {
	hash uint32
	idx  int // 在 sorted 实例数组中的下标
}

func (c *consistentHash) Pick(req Request, instances []*discovery.Instance) (*discovery.Instance, error) {
	if len(instances) == 0 {
		return nil, ErrNoHealthyInstance
	}
	sorted := sortedByID(instances)
	ring := make([]ringPoint, 0, len(sorted)*virtualNodes)
	for idx, in := range sorted {
		replicas := virtualNodes
		if w, ok := c.weights[in.ID]; ok && w > 0 {
			// 权重映射为虚拟节点倍数（1..8），避免单实例权重过大撑爆环。
			replicas = virtualNodes * min(8, max(1, w))
		}
		for v := 0; v < replicas; v++ {
			h := fnv.New32a()
			_, _ = h.Write([]byte(in.ID + "#" + strconv.Itoa(v)))
			ring = append(ring, ringPoint{hash: h.Sum32(), idx: idx})
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		if ring[i].hash != ring[j].hash {
			return ring[i].hash < ring[j].hash
		}
		return ring[i].idx < ring[j].idx
	})

	key := req.HashKey
	if key == "" {
		key = req.Service
	}
	kh := fnv.New32a()
	_, _ = kh.Write([]byte(key))
	target := kh.Sum32()

	pos := sort.Search(len(ring), func(i int) bool { return ring[i].hash >= target })
	if pos == len(ring) {
		pos = 0
	}
	return sorted[ring[pos].idx], nil
}
