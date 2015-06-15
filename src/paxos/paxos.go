package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

//import "time"
//import "strconv"
import "errors"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	instances	map[int]*AcceptorState	//seq, {maxPrepare, acceptP, decided}
	doneInsts	[]int
	pnums		map[int]int	//seq, proposal number
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

func (px *Paxos) myMin() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	minDone := px.doneInsts[px.me]
	for _, value := range px.doneInsts {
		if value < minDone {
			minDone = value
		}
	}

	return minDone + 1
}

func (px *Paxos) fileWriteInstance(seq int, acceptP *AcceptorState) error {

}

func (px *Paxos) fileReadInsts() map[int]*AcceptorState {


}






func (px *Paxos) Proposer(seq int, v interface{}) {

	for !px.isdead() {

		if seq < px.myMin() {
			break
		}

		length := len(px.peers)

		acceptors := make(map[int]string)
		i := 0
		for i < length {
			acceptors[i] = px.peers[i]
			i++
		}

		px.mu.Lock()
		pn := (px.pnums[seq] / length + 1) * length + px.me
		px.pnums[seq] = pn
		px.mu.Unlock()

		var pv interface{}
		sendP := Proposal{pn, pv}

		args := &AcceptorArgs{seq, "Prepare", sendP}
		var reply AcceptorReply

		maxn := 0
		pv = v
		resp := 0

		for key, value := range px.peers {

			ok := false

			if key == px.me {
				if px.Acceptor(args, &reply) == nil {
					ok = true
				}
			} else {
				ok = call(value, "Paxos.Acceptor", args, &reply)
			}

			if ok {
				resp++
				if reply.AcceptP.Num > maxn {
					maxn = reply.AcceptP.Num
					pv = reply.AcceptP.Value
				}
				if resp > length / 2 {
					break
				}
			} else {
				delete(acceptors, key)
				px.mu.Lock()
				if reply.Pnum > px.pnums[seq] {
					px.pnums[seq] = reply.Pnum
				}
				px.mu.Unlock()
			}
		}

		if resp > length / 2 {

			sendP = Proposal{pn, pv}
			args = &AcceptorArgs{seq, "Accept", sendP}
			resp = 0

			for index, value := range acceptors {

				ok := false

				if index == px.me {
					if px.Acceptor(args, &reply) == nil {
						ok = true
					}
				} else {
					ok = call(value, "Paxos.Acceptor", args, &reply)
				}

				if ok {
					resp++
					if resp > length / 2 {
						break
					}
				} else {
					px.mu.Lock()
					if reply.Pnum > px.pnums[seq] {
						px.pnums[seq] = reply.Pnum
					}
					px.mu.Unlock()
				}
			}

			if resp > length / 2 {

				args := &LearnerArgs{seq, sendP, px.me, px.doneInsts[px.me]}
				var reply LearnerReply

				for index, value := range px.peers {
					if index == px.me {
						px.Learner(args, &reply)
					} else {
						call(value, "Paxos.Learner", args, &reply)
						//if call(value, "Paxos.Learner", args, &reply) {
						//	px.doneInsts[index] = reply.Done
						//}
					}
				}

				break
			}
		}
	}
}

func (px *Paxos) Acceptor(args *AcceptorArgs, reply *AcceptorReply) error {

	if args.Seq < px.myMin() {
		return errors.New("instance forgotten")
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	seq, phase, sendP := args.Seq, args.Phase, args.SendP
	info, ok := px.instances[seq]

	if phase == "Prepare" {
		if !ok {
			px.instances[seq] = &AcceptorState{sendP.Num, Proposal{0, nil}, Pending}
		} else {
			if sendP.Num > info.maxPrepare {
				px.instances[seq].maxPrepare = sendP.Num
				reply.AcceptP = info.acceptP
			} else {
				reply.Pnum = info.maxPrepare
				return errors.New("prepare failed")
			}
		}
	} else {	//Accept Phase
		if !ok {
			px.instances[seq] = &AcceptorState{sendP.Num, sendP, Pending}
		} else if sendP.Num >= info.maxPrepare {
			px.instances[seq].maxPrepare = sendP.Num
			px.instances[seq].acceptP = sendP
		} else {
			reply.Pnum = info.maxPrepare
			return errors.New("accept failed")
		}
	}

	return nil
}

func (px *Paxos) Learner(args *LearnerArgs, reply *LearnerReply) error {

	px.mu.Lock()
	defer px.mu.Unlock()

	seq, acceptP, me, done := args.Seq, args.AcceptP, args.Me, args.Done

	if _, ok := px.instances[seq]; !ok {
		px.instances[seq] = &AcceptorState{acceptP.Num, acceptP, Decided}
	} else {
		if acceptP.Num > px.instances[seq].maxPrepare {
			px.instances[seq].maxPrepare = acceptP.Num
		}
		px.instances[seq].decided = Decided
		px.instances[seq].acceptP = acceptP
	}

	px.doneInsts[me] = done
	//reply.Done = px.doneInsts[px.me]

	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go px.Proposer(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq > px.doneInsts[px.me] {
		px.doneInsts[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	maxSeq := -1
	for key, _ := range px.instances {
		if key > maxSeq {
			maxSeq = key
		}
	}

	return maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	minDone := px.doneInsts[px.me]
	for _, value := range px.doneInsts {
		if value < minDone {
			minDone = value
		}
	}

	for key, _ := range px.instances {
		//Out-of-order instances
		if key <= minDone && px.instances[key].decided == Decided {
			delete(px.instances, key)
			delete(px.pnums, key)
		}
	}

	return (minDone + 1)
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.

	//If Status() is called with a sequence number less than Min(),
	//Status() should return Forgotten
	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	if _, ok := px.instances[seq]; !ok {
		return Pending, nil
	}

	return px.instances[seq].decided, px.instances[seq].acceptP.Value
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.instances = make(map[int]*AcceptorState)
	px.doneInsts = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		px.doneInsts[i] = -1
	}
	px.pnums = make(map[int]int)

	//restart & read
	px.instances = px.fileReadInsts()
	px.doneInsts = px.fileReadDone(len(peers))
	px.pnums = px.fileReadPnums()


	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
