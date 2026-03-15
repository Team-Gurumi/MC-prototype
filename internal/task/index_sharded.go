package task

import "fmt"

// Number of shards used to distribute the task index
const TaskIndexShardCount = 32

// Key for a single shard: same prefix as KeyIndex with /<shard> appended.
func KeyIndexShard(ns string, shard int) string {
	return fmt.Sprintf("ns/%s/task/index/%02d", ns, shard)
}

// Returns all shard keys for iteration
func AllIndexShards(ns string) []string {
	out := make([]string, 0, TaskIndexShardCount)
	for i := 0; i < TaskIndexShardCount; i++ {
		out = append(out, KeyIndexShard(ns, i))
	}
	return out
}
