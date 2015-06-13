package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrOldConfig  = "ErrOldConfig"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpSeq	int
	Client	int64
	Shard	int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpSeq	int
	Client	int64
	Shard	int
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	CfgNum	int
	Shard	int
}

type GetShardReply struct {
	DataShard	map[string]string
	RecOps		map[int64]LastOp
	Err			Err
}
