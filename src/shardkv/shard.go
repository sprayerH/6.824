package shardkv

type Shard struct {
	KVStore map[string]string
	Status  ShardStatus
}

func NewShard() *Shard {
	return &Shard{
		KVStore: make(map[string]string),
		Status:  Serving,
	}
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KVStore[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) Err {
	shard.KVStore[key] = value
	return OK
}

func (shard *Shard) Append(key, value string) Err {
	shard.KVStore[key] += value
	return OK
}

func (shard *Shard) deepCopy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range shard.KVStore {
		newShard[k] = v
	}
	return newShard
}
