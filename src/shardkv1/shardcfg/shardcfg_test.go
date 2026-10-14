package shardcfg

import (
	"testing"

	tester "6.5840/tester1"
)

func check_same_config(t *testing.T, c1 ShardConfig, c2 ShardConfig) {
	if c1.ConfigVersion != c2.ConfigVersion {
		t.Fatalf("Num wrong")
	}
	if c1.GroupIdOfShard != c2.GroupIdOfShard {
		t.Fatalf("Shards wrong")
	}
	if len(c1.NameArrOfGroup) != len(c2.NameArrOfGroup) {
		t.Fatalf("number of Groups is wrong")
	}
	for gid, sa := range c1.NameArrOfGroup {
		sa1, ok := c2.NameArrOfGroup[gid]
		if ok == false || len(sa1) != len(sa) {
			t.Fatalf("len(Groups) wrong")
		}
		if ok && len(sa1) == len(sa) {
			for j := 0; j < len(sa); j++ {
				if sa[j] != sa1[j] {
					t.Fatalf("Groups wrong")
				}
			}
		}
	}
}

func TestBasic(t *testing.T) {
	const (
		Gid1 = 1
		Gid2 = 2
	)
	cfg := MakeShardConfig()
	cfg.CheckConfig(t, []tester.ShardGroupId{})

	cfg.JoinBalance(map[tester.ShardGroupId][]string{Gid1: []string{"x", "y", "z"}})
	cfg.CheckConfig(t, []tester.ShardGroupId{Gid1})

	cfg.JoinBalance(map[tester.ShardGroupId][]string{Gid2: []string{"a", "b", "c"}})
	cfg.CheckConfig(t, []tester.ShardGroupId{Gid1, Gid2})

	sa1 := cfg.NameArrOfGroup[Gid1]
	if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", Gid1, sa1)
	}
	sa2 := cfg.NameArrOfGroup[Gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", Gid2, sa2)
	}

	cfg.LeaveBalance([]tester.ShardGroupId{Gid1})
	cfg.CheckConfig(t, []tester.ShardGroupId{Gid2})

	cfg.LeaveBalance([]tester.ShardGroupId{Gid2})
	cfg.CheckConfig(t, []tester.ShardGroupId{})
}
