package shardctrler

import (
	"sort"
)

type CtrlerStateMachine struct {
	Me      int
	Configs []Config
}

func NewCtrlerStateMachine(me int) *CtrlerStateMachine {
	cf := &CtrlerStateMachine{Me: me, Configs: make([]Config, 1)}
	cf.Configs[0] = Config{Groups: make(map[int][]string)}
	return cf
}

func (csm *CtrlerStateMachine) Query(num int) (Config, Err) {
	if num < 0 || num >= len(csm.Configs) {
		return csm.Configs[len(csm.Configs)-1], OK
	}
	return csm.Configs[num], OK
}

// Join 添加一个Group到集群中，需要处理添加完成后的负载均衡问题
// 我们的做法简单来说是从拥有最多 shard 的 Group 中取出一个 shard，
// 将其分配给最少 shard 的那个 Group，如果最多和最少的 shard 的差值小于等于 1，
// 那么说明就已经达到了平衡，否则的话就按照同样的方法一直重复移动 shard。
func (csm *CtrlerStateMachine) Join(groups map[int][]string) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// 构造 gid -> shard 的映射关系
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}
	//raft.LOG(csm.Me, -1, raft.DServer, "Joining pre shards %v gidToShard %v", newConfig.Shards, gidToShards)
	// shard迁移
	for {
		maxGid, minGid := gidWithMaxShards(gidToShards), gidWithMinShards(gidToShards)
		if maxGid != 0 && len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
			break
		}
		// 将最多shard的gid移动一个到最少shard的gid里
		gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
		gidToShards[maxGid] = gidToShards[maxGid][1:]
		//raft.LOG(csm.Me, -1, raft.DServer, "Joining moving gidToShard %v max %v, min %v",
		//	gidToShards, maxGid, minGid)
	}
	var newShards [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	//raft.LOG(csm.Me, -1, raft.DServer, "Joining new shards %v", newConfig.Shards)
	newConfig.Shards = newShards
	csm.Configs = append(csm.Configs, newConfig)
	return OK

}

// Leave 方法是将一个或多个 Group 从集群中删除，和 Join 一样，
// 在删除掉集群中的 Group 之后，其负责的 shard 应该转移到其他的 Group 中，重新让集群达到均衡。
//
// 处理的逻辑和 Join 类似，首先我们将 gid 进行遍历，并将其从 Config 的 `Groups` 中删除，
// 并且记录这些被删除的 gid 所对应的 shard，然后将这些 shard 分配给拥有最少 shard 的 Group。
func (csm *CtrlerStateMachine) Leave(gids []int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// 删除对应的gid， 并将相应的shard暂存起来
	var unassignShards []int
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := gidToShards[gid]; ok {
			unassignShards = append(unassignShards, shards...)
			delete(gidToShards, gid)
		}
	}

	var newShards [NShards]int
	// 重新分配被删除的gid对应的shard
	if len(newConfig.Groups) != 0 {
		for _, shard := range unassignShards {
			minGid := gidWithMinShards(gidToShards)
			gidToShards[minGid] = append(gidToShards[minGid], shard)
		}

		for gid, shards := range gidToShards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	csm.Configs = append(csm.Configs, newConfig)
	return OK

}

func (csm *CtrlerStateMachine) Move(shardId, gid int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}
	newConfig.Shards[shardId] = gid
	csm.Configs = append(csm.Configs, newConfig)
	return OK
}

func copyGroups(groups map[int][]string) map[int][]string {
	newGroup := make(map[int][]string, len(groups))
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroup[gid] = newServers
	}
	return newGroup
}

func gidWithMaxShards(gidToShards map[int][]int) int {
	if shard, ok := gidToShards[0]; ok && len(shard) > 0 {
		return 0
	}

	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxGid, maxShards := -1, -1
	for _, gid := range gids {
		if len(gidToShards[gid]) > maxShards {
			maxGid, maxShards = gid, len(gidToShards[gid])
		}
	}
	return maxGid
}

func gidWithMinShards(gidToShards map[int][]int) int {
	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, minShards := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidToShards[gid]) < minShards {
			minGid, minShards = gid, len(gidToShards[gid])
		}
	}
	return minGid
}
