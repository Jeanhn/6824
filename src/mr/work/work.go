package work

import (
	"fmt"
	"hash/fnv"
)

const (
	BLOCK_SIZE_LIMIT = 10
)

type KeyValue struct {
	Key   string
	Value string
}

func (kv KeyValue) size() int {
	return len(kv.Key) + len(kv.Value)
}

func (kv KeyValue) fmt() string {
	return fmt.Sprintf("%v %v\r\n", kv.Key, kv.Value)
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
