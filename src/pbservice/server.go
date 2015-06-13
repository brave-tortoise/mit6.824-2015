package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"

type PBServer struct {
	mu			sync.Mutex
	l			net.Listener
	dead		int32 // for testing
	unreliable	int32 // for testing
	me			string
	vs			*viewservice.Clerk
	// Your declarations here.
	curview		viewservice.View
	database	map[string]string //key, value
	recOps		map[int64]string  //each clerk has only one outstanding Put or Get
	finished    bool //backup initialize
}

func (pb *PBServer) GetForWard(args *GetForwardArgs, reply *ForwardReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.curview.Backup {
		return errors.New("Get: I'm not backup!")
	}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.curview.Primary {
		return errors.New("Get: I'm not primary!")
	}

	key, client, opid := args.Key, args.Me, args.Opid

	//at-most-once semantics
	if pb.recOps[client] == opid {
		reply.Value = pb.database[key]
		return nil
	}

	if pb.curview.Backup != "" {
		fwdArgs := &GetForwardArgs{key}
		var fwdReply ForwardReply

		ok := call(pb.curview.Backup, "PBServer.GetForWard", fwdArgs, &fwdReply)

		if !ok {
			return errors.New("Get Forward Error")
		}
	}

	reply.Value = pb.database[key]
	pb.recOps[client] = opid

	return nil
}

func (pb *PBServer) PutForWard(args *PutForwardArgs, reply *ForwardReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.curview.Backup {
		return errors.New("Put: I'm not Backup!")
	}

	key, value, op, client, opid := args.Key, args.Value, args.Op, args.Me, args.Opid

	if pb.recOps[client] == opid {
		return nil
	}

	if op == "Append" {
		value = pb.database[key] + value
	}

	pb.database[key] = value
	pb.recOps[client] = opid

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.curview.Primary {
		return errors.New("Put: I'm not primary!")
	}

	if !pb.finished {
        return errors.New("Copy Not Done")
    }

	key, value, op, client, opid := args.Key, args.Value, args.Op, args.Me, args.Opid

	//at-most-once semantics
	if pb.recOps[client] == opid {
		return nil
	}

	if pb.curview.Backup != "" {
		fwdArgs := &PutForwardArgs{key, value, op, client, opid}
		var fwdReply ForwardReply

		curview := pb.curview
		for{
			ok := call(curview.Backup, "PBServer.PutForWard", fwdArgs, &fwdReply)
			if ok {
				break
			} else {
				view, ok2 := pb.vs.Ping(curview.Viewnum)
				if ok2 == nil {
					curview = view
				}
				continue
			}
		}
	}

	if op == "Append" {
		value = pb.database[key] + value
	}

	pb.database[key] = value
	pb.recOps[client] = opid

	return nil
}

func (pb *PBServer) Copydata(args *CopyArgs, reply *CopyReply) error{

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.curview.Backup {
		return errors.New("Copy: I'm not backup!")
	}

	for key,value := range args.Database{
		pb.database[key] = value
	}

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view, ok := pb.vs.Ping(pb.curview.Viewnum)

	if ok == nil {

        flag := (pb.me == view.Primary && view.Backup != "" && pb.curview.Backup != view.Backup)

        pb.curview = view

        if flag {
            pb.finished = false

            go func(){
                copyArgs := &CopyArgs{pb.database}
                var copyReply CopyReply

                for {
                    ok2 := call(view.Backup, "PBServer.Copydata", copyArgs, &copyReply)
                    if ok2 {
                        pb.finished = true
                        break
                    } else {
                        continue
                    }
                }
            }()
        }
    }
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.curview = viewservice.View{0, "", ""}
	pb.database = make(map[string]string)
	pb.recOps = make(map[int64]string)
	pb.finished = true

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
