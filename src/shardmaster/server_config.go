package shardmaster

import (
	"sort"
)

func (c *Config) isInitial() bool {
	for _, gid := range c.Shards {
		if gid != 0 {
			return false
		}
	}
	return true
}

func (c *Config) joinNewGroupsAndRebalanceShards(newGroups map[int][]string) {
	numOfGroups := len(c.Groups)
	for gid, servers := range newGroups {
		if _, ok := c.Groups[gid]; !ok {
			numOfGroups++
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			c.Groups[gid] = newServers
		}
	}

	if numOfGroups == 0 {
		return
	}

	c.rebalanceShards()
}

func (c *Config) leaveGroupsAndReassignShards(gids []int) {
	numOfGroups := len(c.Groups)
	for _, gid := range gids {
		if _, ok := c.Groups[gid]; ok {
			delete(c.Groups, gid)
			numOfGroups--
		}
	}
	if numOfGroups == 0 {
		c.Shards = [NShards]int{}
		c.Groups = map[int][]string{}
		return
	}

	c.rebalanceShards()
}

func (c *Config) rebalanceShards() {
	freeShards := []int{}
	gid2Shards := map[int][]int{}

	numOfGroups := len(c.Groups)

	avgNum := NShards / numOfGroups
	// 由于不能绝对平均 所以有些group会拥有大于
	greaterNum := NShards % numOfGroups

	for gid := range c.Groups {
		gid2Shards[gid] = []int{}
	}

	// 遍历旧shards数组
	for shard, gid := range c.Shards {
		// 回收孤儿shard
		if shards, ok := gid2Shards[gid]; !ok {
			freeShards = append(freeShards, shard)
		} else {
			// 损有余
			if len(shards) > avgNum || (len(shards) == avgNum && greaterNum <= 0) {
				freeShards = append(freeShards, shard)
			} else {
				if len(shards) == avgNum && greaterNum > 0 {
					greaterNum--
				}
				gid2Shards[gid] = append(gid2Shards[gid], shard)
			}
		}
	}

	// 排序 用于遍历map 防止多个raft执行这一段代码时遍历顺序不一致导致shard分配不一致
	var keys []int
	for k := range gid2Shards {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	freeShardIndex := 0
	for _, k := range keys {
		shards := gid2Shards[k]
		// 补不足
		if len(shards) < avgNum {
			for i := 0; i < avgNum-len(shards); i++ {
				c.Shards[freeShards[freeShardIndex]] = k
				freeShardIndex++
			}
		}
	}
	j := 0
	for i := freeShardIndex; i < len(freeShards); i++ {
		if _, ok := c.Groups[c.Shards[freeShards[i]]]; !ok {
			c.Shards[freeShards[i]] = keys[j]
			j++
		}
	}
}
