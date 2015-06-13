package kvpaxos

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


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key		string
	Value	string
	OpType	string
	OpId	int
	Client	int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seq			int
	recOps		map[int64]int
	database	map[string]string
}

func (kv *KVPaxos) WaitForDecide(seq int) interface{} {
	to := 10 * time.Millisecond
	for {
		decided, value := kv.px.Status(seq)
		if decided == paxos.Decided {
			return value
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, opId, client := args.Key, args.OpId, args.Client
	op := Op{key, "", "Get", opId, client}
	seq := kv.seq

	if opId <= kv.recOps[client] {
		reply.Value = kv.database[key]
		return nil
	}

	for {
		state, decidedV := kv.px.Status(seq)
		if state != paxos.Decided {
			kv.px.Start(seq, op)
			decidedV = kv.WaitForDecide(seq)
		}
		decidedOp := decidedV.(Op)

		//if decidedOp.OpId > kv.recOps[decidedOp.Client] {
			if decidedOp.OpType == "Put" {
				kv.database[decidedOp.Key] = decidedOp.Value
			} else if decidedOp.OpType == "Append" {
				kv.database[decidedOp.Key] = kv.database[decidedOp.Key] + decidedOp.Value
			}
			kv.px.Done(seq)
			kv.recOps[decidedOp.Client] = decidedOp.OpId
		//} else {
			//os.Exit(-1)
		//}

		if decidedOp != op {
			seq++
		} else {
			break
		}
	}

	reply.Value = kv.database[key]
	kv.seq = seq + 1

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, value, opType, opId, client := args.Key, args.Value, args.OpType, args.OpId, args.Client
	op := Op{key, value, opType, opId, client}
	seq := kv.seq

	if opId <= kv.recOps[client] {
		return nil
	}

	for {
		state, decidedV := kv.px.Status(seq)
		if state != paxos.Decided {
			kv.px.Start(seq, op)
			decidedV = kv.WaitForDecide(seq)
		}
		decidedOp := decidedV.(Op)

		//if decidedOp.OpId > kv.recOps[decidedOp.Client] {
			if decidedOp.OpType == "Put" {
				kv.database[decidedOp.Key] = decidedOp.Value
			} else if decidedOp.OpType == "Append" {
				kv.database[decidedOp.Key] = kv.database[decidedOp.Key] + decidedOp.Value
			}
			kv.px.Done(seq)
			kv.recOps[decidedOp.Client] = decidedOp.OpId
		//} else {
		//	os.Exit(-1)
		//}

		if decidedOp != op {
			seq++
		} else {
			break
		}
	}

	kv.seq = seq + 1

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.seq = 0
	kv.recOps = make(map[int64]int)
	kv.database = make(map[string]string)


	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
