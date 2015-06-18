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

import "errors"
import "strconv"
import "encoding/gob"
import "bytes"
import "io/ioutil"
//import "unsafe"


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
	dir			string
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

func (px *Paxos) instanceDir() string {
	d := px.dir + "/instances/"
	// create directory if needed
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

func (px *Paxos) encodeAcceptP(info AcceptorState) string {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	/*fmt.Println("1connection pointer is", acceptP)
	strPointerInt := fmt.Sprintf("%d", unsafe.Pointer(acceptP))
	fmt.Printf("1int: %s\n", strPointerInt)
	strPointerHex := fmt.Sprintf("%p", unsafe.Pointer(acceptP))
    fmt.Println("1connection is", strPointerHex)
	e.Encode(strPointerInt)*/
	e.Encode(info)
	return string(w.Bytes())
}

func (px *Paxos) decodeAcceptP(buf string) AcceptorState {
	r := bytes.NewBuffer([]byte(buf))
	d := gob.NewDecoder(r)
	//var acceptP *AcceptorState
	/*var strPointerInt string
	d.Decode(&strPointerInt)
	fmt.Printf("2int: %s\n", strPointerInt)
	i, _ := strconv.ParseInt(strPointerInt, 10, 0)
	var acceptP *AcceptorState
	acceptP = *(**AcceptorState)(unsafe.Pointer(&i))
	fmt.Println("2connection pointer is", acceptP)
	debugMsg := fmt.Sprintf("%p", unsafe.Pointer(acceptP))
    fmt.Println("debugMsg is", debugMsg)*/
	var info AcceptorState
	d.Decode(&info)
	return info
}

func (px *Paxos) fileWriteInstance(seq int) error {
	if px.dir == "" {
		return nil
	}
	fullname := px.instanceDir() + "/seq-" + strconv.Itoa(seq)
	tempname := px.instanceDir() + "/temp-" + strconv.Itoa(seq)
	content := px.encodeAcceptP(*px.instances[seq])
	//fmt.Println("content: ", content)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

func (px *Paxos) fileReadInstance(seq int) (AcceptorState, error) {
	info := AcceptorState{}
	fullname := px.instanceDir() + "/seq-" + strconv.Itoa(seq)
	content, err := ioutil.ReadFile(fullname)
	if err == nil {
		info = px.decodeAcceptP(string(content))
	}
	return info, err
}

func (px *Paxos) fileReadInsts() map[int]*AcceptorState {
	instances := map[int]*AcceptorState{}
	d := px.instanceDir()
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadInsts could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "seq-" {
			seq, err := strconv.Atoi(n1[4:])
			if err != nil {
				log.Fatalf("fileReadInsts bad file name %v: %v", n1, err)
			}
			info, err := px.fileReadInstance(seq)
			if err != nil {
				log.Fatalf("fileReadInsts fileReadInstance failed for %v: %v", seq, err)
			}
			instances[seq] = &info
		}
	}
	return instances
}

func (px *Paxos) fileDeleteInstance(seq int) {
	//fmt.Printf("%d: delete %d\n", px.me, seq)
	fullname := px.instanceDir() + "/seq-" + strconv.Itoa(seq)
	os.Remove(fullname)
}


func (px *Paxos) doneDir() string {
	d := px.dir + "/dones/"
	// create directory if needed
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

func (px *Paxos) fileWriteDone(me int, done int) error {
	if px.dir == "" {
		return nil
	}
	fullname := px.doneDir() + "/me-" + strconv.Itoa(me)
	tempname := px.doneDir() + "/temp-" + strconv.Itoa(me)
	content := strconv.Itoa(done)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

func (px *Paxos) fileReadDone(me int) (int, error) {
	done := -1
	fullname := px.doneDir() + "/me-" + strconv.Itoa(me)
	content, err := ioutil.ReadFile(fullname)
	if err == nil {
		done, _ = strconv.Atoi(string(content))
	}
	return done, err
}

func (px *Paxos) fileReadDones(num int) []int {
	doneInsts := make([]int, num)
	for i := 0; i < num; i++ {
		doneInsts[i] = -1
	}
	//instances := map[int]*AcceptorState{}
	d := px.doneDir()
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadDones could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:3] == "me-" {
			me, err := strconv.Atoi(n1[3:])
			if err != nil {
				log.Fatalf("fileReadDones bad file name %v: %v", n1, err)
			}
			done, err := px.fileReadDone(me)
			if err != nil {
				log.Fatalf("fileReadDones fileReadDone failed for %v: %v", me, err)
			}
			doneInsts[me] = done
		}
	}
	return doneInsts
}
/*
func (px *Paxos) pnumDir() string {
	d := px.dir + "/pnums/"
	// create directory if needed
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

func (px *Paxos) fileWritePnum(seq int, pnum int) error {
	if px.dir == "" {
		return nil
	}
	fullname := px.pnumDir() + "/seq-" + strconv.Itoa(seq)
	tempname := px.pnumDir() + "/temp-" + strconv.Itoa(seq)
	content := strconv.Itoa(pnum)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

func (px *Paxos) fileReadPnum(seq int) (int, error) {
	pn := 0
	fullname := px.pnumDir() + "/seq-" + strconv.Itoa(seq)
	content, err := ioutil.ReadFile(fullname)
	if err == nil {
		pn, _ = strconv.Atoi(string(content))
	}
	return pn, err
}

func (px *Paxos) fileReadPnums() map[int]int {
	pnums := map[int]int{}
	d := px.pnumDir()
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadPnums could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "seq-" {
			seq, err := strconv.Atoi(n1[4:])
			if err != nil {
				log.Fatalf("fileReadPnums bad file name %v: %v", n1, err)
			}
			pn, err := px.fileReadPnum(seq)
			if err != nil {
				log.Fatalf("fileReadPnums fileReadPnum failed for %v: %v", seq, err)
			}
			pnums[seq] = pn
		}
	}
	return pnums
}
*/
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
		//px.fileWritePnum(seq, pn)
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
					//px.fileWritePnum(seq, reply.Pnum)
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
						//px.fileWritePnum(seq, reply.Pnum)
					}
					px.mu.Unlock()
				}
			}

			if resp > length / 2 {
				for index, value := range px.peers {
					args := &LearnerArgs{seq, sendP, px.doneInsts}
					var reply LearnerReply
					if index == px.me {
						px.Learner(args, &reply)
					} else {
						if call(value, "Paxos.Learner", args, &reply) {
							px.mu.Lock()
							if reply.Done > px.doneInsts[index] {
								px.doneInsts[index] = reply.Done
								px.fileWriteDone(index, reply.Done)
							}
							px.mu.Unlock()
						}
					}
				}

				break
			}
		}
	}
}

func (px *Paxos) Acceptor(args *AcceptorArgs, reply *AcceptorReply) error {

	if args.Seq < px.myMin() {
		//fmt.Printf("%d: forgotten %d < %d\n", px.me, args.Seq, px.myMin())
		return errors.New("instance forgotten")
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	seq, phase, sendP := args.Seq, args.Phase, args.SendP
	info, ok := px.instances[seq]

	if phase == "Prepare" {
		if !ok {
			px.instances[seq] = &AcceptorState{sendP.Num, Proposal{0, nil}, Pending}
			//px.fileWriteInstance(seq)
		} else {
			if sendP.Num > info.MaxPrepare {
				px.instances[seq].MaxPrepare = sendP.Num
				//px.fileWriteInstance(seq)
				reply.AcceptP = info.AcceptP
			} else {
				reply.Pnum = info.MaxPrepare
				return errors.New("prepare failed")
			}
		}
	} else {	//Accept Phase
		if !ok {
			px.instances[seq] = &AcceptorState{sendP.Num, sendP, Pending}
			//px.fileWriteInstance(seq)
		} else if sendP.Num >= info.MaxPrepare {
			px.instances[seq].MaxPrepare = sendP.Num
			px.instances[seq].AcceptP = sendP
			//px.fileWriteInstance(seq)
		} else {
			reply.Pnum = info.MaxPrepare
			return errors.New("accept failed")
		}
	}

	return nil
}

func (px *Paxos) Learner(args *LearnerArgs, reply *LearnerReply) error {

	px.mu.Lock()
	defer px.mu.Unlock()

	seq, acceptP := args.Seq, args.AcceptP

	if _, ok := px.instances[seq]; !ok {
		px.instances[seq] = &AcceptorState{acceptP.Num, acceptP, Decided}
	} else {
		if acceptP.Num > px.instances[seq].MaxPrepare {
			px.instances[seq].MaxPrepare = acceptP.Num
		}
		px.instances[seq].Decided = Decided
		px.instances[seq].AcceptP = acceptP
	}

	px.fileWriteInstance(seq)

	for key, value := range args.DoneInsts {
		if value > px.doneInsts[key] {
			px.doneInsts[key] = value
			px.fileWriteDone(key, value)
		}
	}
	reply.Done = px.doneInsts[px.me]

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
		px.fileWriteDone(px.me, seq)
	}

	//fmt.Printf("done: %d\n", seq)
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
		if key <= minDone && px.instances[key].Decided == Decided {
			delete(px.instances, key)
			px.fileDeleteInstance(key)
			delete(px.pnums, key)
		}
	}

	//fmt.Printf("min: %d, %d\n", px.me, minDone)

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

	return px.instances[seq].Decided, px.instances[seq].AcceptP.Value
}

func (px *Paxos) GatherState(args *SyncArgs, reply *SyncReply) error {

	//fmt.Printf("%d: gather paxos\n", px.me)

	//minDone := px.myMin()
    //if minDone <= args.MinDone {
    //    return errors.New("too old")
    //}

    px.mu.Lock()
    defer px.mu.Unlock()

	//fmt.Println("gather paxos end")

	//reply.MinDone = minDone
	reply.Instances = map[int]Proposal{}
    for key, value := range px.instances {
		reply.Instances[key] = value.AcceptP
    }
    reply.DoneInsts = px.doneInsts
    //reply.Pnums = px.pnums

	return nil
}

func (px *Paxos) SyncState(peers []string) {

	//minDone := px.myMin()
	args := SyncArgs{}
	var reply SyncReply
	for {
		for idx, sv := range peers {
			if idx == px.me {
				continue
			}
		    if call(sv, "Paxos.GatherState", args, &reply) {
				if reply.DoneInsts[px.me] > px.doneInsts[px.me] {
					//minDone = reply.MinDone
					for key, value := range reply.Instances {
						px.instances[key] = &AcceptorState{0, value, Decided}
		            }
		            for index, value := range reply.DoneInsts {
						px.doneInsts[index] = value
		            }
				}
				return
			}
		}
	}
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
func Make(peers []string, me int, rpcs *rpc.Server, dir string, restart bool) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.dir = dir

	// Your initialization code here.
	if restart {
		px.instances = px.fileReadInsts()
		px.doneInsts = px.fileReadDones(len(peers))
		//px.pnums = px.fileReadPnums()
		if px.doneInsts[px.me] == -1 {
			px.SyncState(peers)
		}
	} else {
		px.instances = make(map[int]*AcceptorState)
		px.doneInsts = make([]int, len(peers))
		for i := 0; i < len(peers); i++ {
			px.doneInsts[i] = -1
		}
	}

	px.pnums = make(map[int]int)


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
