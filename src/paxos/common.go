package paxos

type Proposal struct {
	Num			int
	Value		interface{}
}

type AcceptorState struct {
	MaxPrepare	int
	AcceptP		Proposal
	Decided		Fate
}

type AcceptorArgs struct {
	Seq			int
	Phase		string
	SendP		Proposal
}

type AcceptorReply struct {
	AcceptP		Proposal
	Pnum		int
}

type LearnerArgs struct {
	Seq			int
	AcceptP		Proposal
	DoneInsts	[]int
}

type LearnerReply struct {
	Done		int
}

type SyncArgs struct {
}

type SyncReply struct {
	Instances   map[int]Proposal
	DoneInsts   []int
}
