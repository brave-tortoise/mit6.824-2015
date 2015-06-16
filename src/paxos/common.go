package paxos

type Proposal struct {
	Num			int
	Value		interface{}
}

type AcceptorState struct {
	MaxPrepare	int
	AcceptP		Proposal
	//Num			int
	//Value		interface{}
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
	Me			int
	Done		int
}

type LearnerReply struct {
}
