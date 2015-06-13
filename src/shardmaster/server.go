package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

import "time"
//import "runtime/debug"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs	[]Config // indexed by config num
	seq		int
}

type Op struct {
	// Your data here.
	OpType	string
	OpId	int64
	GID		int64
	Servers	[]string
	Shard	int
}

func (sm *ShardMaster) WaitForDecide(seq int) interface{} {
	to := 10 * time.Millisecond
	for {
		decided, value := sm.px.Status(seq)
		if decided == paxos.Decided {
			return value
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) CheckValid(c Config) {
	if len(c.Groups) > 0 {
		for _, g := range c.Shards {
			_, ok := c.Groups[g]
			if ok == false {
				fmt.Println("Not valid result, unallocated shards", c.Num)
				fmt.Println("len(groups): ", len(c.Groups))
				//debug.PrintStack()
				os.Exit(-1)
			}
		}
	}
}

func (sm *ShardMaster) CheckBalanced(c Config) {
	// more or less balanced sharding?
	counts := map[int64]int{}
	for _, g := range c.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range c.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		fmt.Println("max %v too much larger than min %v", max, min)
		os.Exit(-1)
	}
}

func (sm *ShardMaster) CreateNewCfg(oldCfg *Config) *Config {

	var newCfg Config
	newCfg.Num = oldCfg.Num + 1
	newCfg.Shards = oldCfg.Shards
	newCfg.Groups = map[int64][]string{}

	for gid, servers := range oldCfg.Groups {
		newCfg.Groups[gid] = servers
	}

	sm.configs = append(sm.configs, newCfg)

	return &sm.configs[len(sm.configs) - 1]
}

func (sm *ShardMaster) MovedShard(cfg *Config, gid int64) int {
	for idx, g := range cfg.Shards {
		if g == gid {
			return idx
		}
	}
	return -1
}

func (sm *ShardMaster) DivideShards(cfg *Config, gid int64) {

	groupNum := len(cfg.Groups)
	if groupNum == 1 {
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = gid
		}
		return
	}

	newAvg := NShards / groupNum
	if newAvg == 0 {
		return
	}

	shardsNum := map[int64]int{}
	for _, g := range cfg.Shards {
		shardsNum[g]++
	}

	movedNum := newAvg
	//fmt.Printf("div: groupnum: %d\n", len(shardsNum))
	//fmt.Printf("div: movedNum: %d, avg: %d\n", movedNum, newAvg)
	flag := true
	for flag {
		flag = false
		for g, n := range shardsNum {
			if n > newAvg + 1 {
				idx := sm.MovedShard(cfg, g)
				cfg.Shards[idx] = gid
				shardsNum[g] = n - 1
				movedNum--
				flag = true
			}
		}
	}

	if movedNum == 0 {
		return
	}

	for g, n := range shardsNum {
		if n > newAvg {
			idx := sm.MovedShard(cfg, g)
			cfg.Shards[idx] = gid
			movedNum--
			if movedNum == 0 {
				return
			}
		}
	}

	//rebalance the effects of MOVE?
	//	...
}

func (sm *ShardMaster) HandOffShards(cfg *Config, gid int64) {

	groupNum := len(cfg.Groups)
	if groupNum == 0 {
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	shardsNum := map[int64]int{}
	for g, _ := range cfg.Groups {
		shardsNum[g] = 0
	}

	movedShards := []int{}
	for i, g := range cfg.Shards {
		if g == gid {
			movedShards = append(movedShards, i)
		} else {
			shardsNum[g]++
		}
	}

	movedNum := len(movedShards)
	if movedNum == 0 {
		return
	}

	newAvg := NShards / groupNum
	//fmt.Printf("hdo: groupnum: %d\n", len(shardsNum))
	//fmt.Printf("hdo: movedNum: %d, avg: %d\n", movedNum, newAvg)
	idx := 0
	flag := true
	for flag {
		flag = false
		for g, n := range shardsNum {
			if n < newAvg {
				cfg.Shards[movedShards[idx]] = g
				idx++
				if idx == movedNum {
					return
				}
				shardsNum[g] = n + 1
				flag = true
			}
		}
	}

	for g, n := range shardsNum {
		if n == newAvg {
			cfg.Shards[movedShards[idx]] = g
			idx++
			if idx == movedNum {
				return
			}
		}
	}
}

func (sm *ShardMaster) Propose(op Op) {

	seq := sm.seq

	for {
		state, decidedV := sm.px.Status(seq)
		if state != paxos.Decided {
			sm.px.Start(seq, op)
			decidedV = sm.WaitForDecide(seq)
		}
		decidedOp := decidedV.(Op)

		if decidedOp.OpType != "Query" {
			gid := decidedOp.GID
			idx := len(sm.configs) - 1
			oldCfg := &sm.configs[idx]

			switch decidedOp.OpType {
			case "Join":
				if _, ok := oldCfg.Groups[gid]; !ok {
					newCfg := sm.CreateNewCfg(oldCfg)
					newCfg.Groups[gid] = decidedOp.Servers
					sm.DivideShards(newCfg, gid)
				}
			case "Leave":
				if _, ok := oldCfg.Groups[gid]; ok {
					newCfg := sm.CreateNewCfg(oldCfg)
					delete(newCfg.Groups, gid)
					sm.HandOffShards(newCfg, gid)
				}
			case "Move":
				newCfg := sm.CreateNewCfg(oldCfg)
				newCfg.Shards[decidedOp.Shard] = gid
			}
		}

		sm.px.Done(seq)

		if decidedOp.OpId != op.OpId {
			seq++
		} else {
			break
		}
	}

	sm.seq = seq + 1
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{OpType:"Join", OpId:nrand(), GID:args.GID, Servers:args.Servers}
	sm.Propose(op)
	//sm.CheckValid(sm.configs[len(sm.configs) - 1])
	//sm.CheckBalanced(sm.configs[len(sm.configs) - 1])

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{OpType:"Leave", OpId:nrand(), GID:args.GID}
	sm.Propose(op)
	//sm.CheckValid(sm.configs[len(sm.configs) - 1])
	//sm.CheckBalanced(sm.configs[len(sm.configs) - 1])

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{OpType:"Move", OpId:nrand(), Shard:args.Shard, GID:args.GID}
	sm.Propose(op)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	num := args.Num
	op := Op{OpType:"Query", OpId:nrand()}

	sm.Propose(op)

	idx := len(sm.configs) - 1
	if num == -1 || num > idx {
		reply.Config = sm.configs[idx]
	} else {
		reply.Config = sm.configs[num]
	}

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.seq = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
