package paxos

type Proposal struct {
	Num			int
	Value		interface{}
}

type AcceptorState struct {
	maxPrepare	int
	acceptP		Proposal
	decided		Fate
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
	//Done		int
}
