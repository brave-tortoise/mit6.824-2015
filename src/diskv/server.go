package diskv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "encoding/base32"
import "math/rand"
import "shardmaster"
import "io/ioutil"
import "strconv"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	OpId		int64
	OpType		string
	OpSeq		int
	Client		int64
	Key			string
	Value		string
	Shard		int
	Config		shardmaster.Config
	CfgNum		int
	DataShard	map[string]string
	GetShards	map[int]map[string]string
	GetRecOps	map[int64]LastOp
}

type LastOp struct {
	OpSeq		int
	Value		string
}

type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

	// Your definitions here.
	database	[]map[string]string	//shard, <key, value>
	seq			int
	recOps		map[int64]LastOp	//client, LastOp
	config		shardmaster.Config
}

//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}

func (kv *DisKV) WaitForDecide(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, value := kv.px.Status(seq)
		if decided == paxos.Decided {
			return value.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (kv *DisKV) Propose(op Op) Err {

	var err Err

	for {
		if op.OpType == "Reconfig" {
			if op.Config.Num <= kv.config.Num {
				return OK
			}
		} else {
			if kv.config.Shards[op.Shard] != kv.gid {
				return ErrWrongGroup
			}
			if lo, ok := kv.recOps[op.Client]; ok {
				if op.OpSeq <= lo.OpSeq {
					return OK
				}
			}
		}

		seq := kv.seq
		state, decidedV := kv.px.Status(seq)
		if state != paxos.Decided {
			kv.px.Start(seq, op)
			decidedV = kv.WaitForDecide(seq)
		}
		decidedOp := decidedV.(Op)

		if decidedOp.OpType == "Reconfig" {
			for shard, data := range decidedOp.GetShards {
				//kv.fileReplaceShard(shard, data)
				for k, v := range data {
					kv.database[shard][k] = v
				}
			}

			for client, lastOp := range decidedOp.GetRecOps {
				lo, ok := kv.recOps[client]
				if !ok || lastOp.OpSeq > lo.OpSeq {
					kv.recOps[client] = lastOp
				}
			}

			kv.config = decidedOp.Config
		} else {
			opType, shard, key, value := decidedOp.OpType, decidedOp.Shard, decidedOp.Key, decidedOp.Value
			err = OK
			lastOp := LastOp{OpSeq:decidedOp.OpSeq}

			if opType == "Put" {
				kv.database[shard][key] = value
			} else if opType == "Append" {
				kv.database[shard][key] = kv.database[shard][key] + value
			} else {
				if v, ok := kv.database[shard][key]; ok {
					lastOp.Value = v
				} else {
					err = ErrNoKey
				}
			}

			if err == OK {
				kv.recOps[decidedOp.Client] = lastOp
			}
		}

		kv.px.Done(seq)
		kv.seq++
		if decidedOp.OpId == op.OpId {
			break
		}
	}

	return err
}

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{OpId:nrand(), OpType:"Get", OpSeq:args.OpSeq, Client:args.Client, Key:args.Key, Shard:args.Shard}
	err := kv.Propose(op)

	reply.Value = kv.database[args.Shard][args.Key]
	reply.Err = err

	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{OpId:nrand(), OpType:args.Op, OpSeq:args.OpSeq, Client:args.Client, Key:args.Key, Value:args.Value, Shard:args.Shard}
	err := kv.Propose(op)

	reply.Err = err

	return nil
}

func (kv *DisKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	if kv.config.Num < args.CfgNum {
		reply.Err = ErrOldConfig
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.DataShard = kv.database[args.Shard]
	reply.RecOps = kv.recOps
	reply.Err = OK

	return nil
}

func (kv *DisKV) ReConfig(newCfg shardmaster.Config) {

	oldCfg := kv.config
	getShards := map[int]map[string]string{}
	getRecOps := map[int64]LastOp{}

	if oldCfg.Num != 0 {
		cfgNum := newCfg.Num
		for shard, gid := range oldCfg.Shards {
			if gid != 0 && newCfg.Shards[shard] == kv.gid && gid != kv.gid {
				servers := oldCfg.Groups[gid]
				args := GetShardArgs{cfgNum, shard}
				var reply GetShardReply

				for _, sv := range servers {
					ok := call(sv, "DisKV.GetShard", args, &reply)
					if ok && reply.Err == OK {
						break
					}
				}

				if reply.Err == OK {
					getShards[shard] = reply.DataShard
					for client, lastOp := range reply.RecOps {
						lo, ok := getRecOps[client]
						if !ok || lastOp.OpSeq > lo.OpSeq {
							getRecOps[client] = lastOp
						}
					}
				} else {
					return
				}
			}
		}
	}

	op := Op{OpId:nrand(), OpType:"Reconfig", Config:(newCfg), GetShards:getShards, GetRecOps:getRecOps}
	kv.Propose(op)
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *DisKV) tick() {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cfgNum := kv.config.Num
	newCfg := kv.sm.Query(cfgNum + 1)

	if newCfg.Num == cfgNum + 1 {
		kv.ReConfig(newCfg)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	// Your initialization code here.
	// Don't call Join().
	kv.database = make([]map[string]string, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.database[i] = make(map[string]string)
	}
	kv.seq = 0
	kv.recOps = make(map[int64]LastOp)
	kv.config = shardmaster.Config{}

	if restart {
		//read file to update mem

	}


	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// log.SetOutput(os.Stdout)



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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
