package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	sendJob := func(worker string, opt JobType, jobNumber int, numOther int) bool {
		args := DoJobArgs{mr.file, opt, jobNumber, numOther}
		var reply DoJobReply
		return call(worker, "Worker.DoJob", args, &reply);
	}

	mapDone := make(chan int, mr.nMap)

	for i:=0; i<mr.nMap; i++ {
		go func(jobNumber int) {
			for {
				var worker string
				done := false
				select {
				case worker = <-mr.registerChannel:
					done = sendJob(worker, Map, jobNumber, mr.nReduce)
				case worker = <-mr.idleChannel:
					done = sendJob(worker, Map, jobNumber, mr.nReduce)
				}
				if(done){
					mapDone <- 0
					mr.idleChannel <- worker
					return
				}
			}
		}(i)
	}

	for i:=0; i<mr.nMap; i++ {
		<- mapDone
	}

	//fmt.Println("map is done!")

////////////////////////////////////////////////////////////////////////

	reduceDone := make(chan int, mr.nReduce)

	for i:=0; i<mr.nReduce; i++ {
		go func(jobNumber int) {
			for {
				var worker string
				done := false
				select {
				case worker = <-mr.registerChannel:
					done = sendJob(worker, Reduce, jobNumber, mr.nMap)
				case worker = <-mr.idleChannel:
					done = sendJob(worker, Reduce, jobNumber, mr.nMap)
				}
				if(done) {
					reduceDone <- 0
					mr.idleChannel <- worker
					return
				}
			}
		}(i)
	}

	for i:=0; i<mr.nReduce; i++ {
		<- reduceDone
	}

	//fmt.Println("reduce is done!")


	return mr.KillWorkers()
}
