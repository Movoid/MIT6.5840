package shardcfg

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"runtime/debug"
	"slices"
	"testing"

	tester "6.5840/tester1"
)

type ShardId int
type ConfigVersion int

const (
	NShards  = 12 // The number of shards.
	NumFirst = ConfigVersion(1)
)

const (
	Gid1 = tester.ShardGroupId(1)
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func ShardIdOfKey(key string) ShardId {
	h := fnv.New32a()
	h.Write([]byte(key))
	shard := ShardId(ShardId(h.Sum32()) % NShards)
	return shard
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type ShardConfig struct {
	ConfigVersion  ConfigVersion                    // config number
	GroupIdOfShard [NShards]tester.ShardGroupId     // shard -> gid
	NameArrOfGroup map[tester.ShardGroupId][]string // gid -> servers[]
}

func MakeShardConfig() *ShardConfig {
	c := &ShardConfig{
		NameArrOfGroup: make(map[tester.ShardGroupId][]string),
	}
	return c
}

func (cfg *ShardConfig) String() string {
	b, err := json.Marshal(cfg)
	if err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return string(b)
}

func FromString(s string) *ShardConfig {
	scfg := &ShardConfig{}
	if err := json.Unmarshal([]byte(s), scfg); err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return scfg
}

func (cfg *ShardConfig) Copy() *ShardConfig {
	c := MakeShardConfig()
	c.ConfigVersion = cfg.ConfigVersion
	c.GroupIdOfShard = cfg.GroupIdOfShard
	for k, srvs := range cfg.NameArrOfGroup {
		s := make([]string, len(srvs))
		copy(s, srvs)
		c.NameArrOfGroup[k] = s
	}
	return c
}

// mostgroup, mostn, leastgroup, leastn
func analyze(c *ShardConfig) (tester.ShardGroupId, int, tester.ShardGroupId, int) {
	counts := map[tester.ShardGroupId]int{}
	for _, g := range c.GroupIdOfShard {
		counts[g] += 1
	}

	mn := -1
	var mg tester.ShardGroupId = -1
	ln := 257
	var lg tester.ShardGroupId = -1
	// Enforce deterministic ordering, map iteration
	// is randomized in go
	groups := make([]tester.ShardGroupId, len(c.NameArrOfGroup))
	i := 0
	for k := range c.NameArrOfGroup {
		groups[i] = k
		i++
	}
	slices.Sort(groups)
	for _, g := range groups {
		if counts[g] < ln {
			ln = counts[g]
			lg = g
		}
		if counts[g] > mn {
			mn = counts[g]
			mg = g
		}
	}

	return mg, mn, lg, ln
}

// return GID of group with least number of
// assigned shards.
func least(c *ShardConfig) tester.ShardGroupId {
	_, _, lg, _ := analyze(c)
	return lg
}

// balance assignment of shards to groups.
// modifies c.
func (c *ShardConfig) Rebalance() {
	// if no groups, un-assign all shards
	if len(c.NameArrOfGroup) < 1 {
		for s, _ := range c.GroupIdOfShard {
			c.GroupIdOfShard[s] = 0
		}
		return
	}

	// assign all unassigned shards
	for s, g := range c.GroupIdOfShard {
		_, ok := c.NameArrOfGroup[g]
		if ok == false {
			lg := least(c)
			c.GroupIdOfShard[s] = lg
		}
	}

	// move shards from most to least heavily loaded
	for {
		mg, mn, lg, ln := analyze(c)
		if mn < ln+2 {
			break
		}
		// move 1 shard from mg to lg
		for s, g := range c.GroupIdOfShard {
			if g == mg {
				c.GroupIdOfShard[s] = lg
				break
			}
		}
	}
}

func (cfg *ShardConfig) Join(servers map[tester.ShardGroupId][]string) bool {
	changed := false
	for gid, servers := range servers {
		_, ok := cfg.NameArrOfGroup[gid]
		if ok {
			log.Printf("re-Join %v", gid)
			return false
		}
		for xgid, xservers := range cfg.NameArrOfGroup {
			for _, s1 := range xservers {
				for _, s2 := range servers {
					if s1 == s2 {
						log.Fatalf("Join(%v) puts server %v in groups %v and %v", gid, s1, xgid, gid)
					}
				}
			}
		}
		// new GID
		// modify cfg to reflect the Join()
		cfg.NameArrOfGroup[gid] = servers
		changed = true
	}
	if changed == false {
		log.Fatalf("Join but no change")
	}
	cfg.ConfigVersion += 1
	return true
}

func (cfg *ShardConfig) Leave(gids []tester.ShardGroupId) bool {
	changed := false
	for _, gid := range gids {
		_, ok := cfg.NameArrOfGroup[gid]
		if ok == false {
			// already no GID!
			log.Printf("Leave(%v) but not in config", gid)
			return false
		} else {
			// modify op.Config to reflect the Leave()
			delete(cfg.NameArrOfGroup, gid)
			changed = true
		}
	}
	if changed == false {
		debug.PrintStack()
		log.Fatalf("Leave but no change")
	}
	cfg.ConfigVersion += 1
	return true
}

func (cfg *ShardConfig) JoinBalance(servers map[tester.ShardGroupId][]string) bool {
	if !cfg.Join(servers) {
		return false
	}
	cfg.Rebalance()
	return true
}

func (cfg *ShardConfig) LeaveBalance(gids []tester.ShardGroupId) bool {
	if !cfg.Leave(gids) {
		return false
	}
	cfg.Rebalance()
	return true
}

func (cfg *ShardConfig) GidServers(sh ShardId) (tester.ShardGroupId, []string, bool) {
	gid := cfg.GroupIdOfShard[sh]
	srvs, ok := cfg.NameArrOfGroup[gid]
	return gid, srvs, ok
}

func (cfg *ShardConfig) IsMember(gid tester.ShardGroupId) bool {
	for _, g := range cfg.GroupIdOfShard {
		if g == gid {
			return true
		}
	}
	return false
}

func (cfg *ShardConfig) CheckConfig(t *testing.T, groups []tester.ShardGroupId) {
	if len(cfg.NameArrOfGroup) != len(groups) {
		fatalf(t, "wanted %v groups, got %v", len(groups), len(cfg.NameArrOfGroup))
	}

	// are the groups as expected?
	for _, g := range groups {
		_, ok := cfg.NameArrOfGroup[g]
		if ok != true {
			fatalf(t, "missing group %v", g)
		}
	}

	// any un-allocated shards?
	if len(groups) > 0 {
		for s, g := range cfg.GroupIdOfShard {
			_, ok := cfg.NameArrOfGroup[g]
			if ok == false {
				fatalf(t, "shard %v -> invalid group %v", s, g)
			}
		}
	}

	// more or less balanced sharding?
	counts := map[tester.ShardGroupId]int{}
	for _, g := range cfg.GroupIdOfShard {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range cfg.NameArrOfGroup {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		fatalf(t, "max %v too much larger than min %v", max, min)
	}
}

func fatalf(t *testing.T, format string, args ...any) {
	debug.PrintStack()
	t.Fatalf(format, args...)
}
