package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId 	  int64
	Seq   int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// Get must from majority
	Op  string
	ClientId  int64
	Seq int64
}

type Reply struct {
	Err   Err
	Value string
}
