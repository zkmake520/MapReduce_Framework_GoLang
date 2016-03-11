package mapreduce

import (
	"fmt"
 )
func (mr *Master) assignJob(worker string,arg * TaskArgs,jobFinishChan chan bool,failedJob chan int){
	go func(worker string,arg *TaskArgs){
		ok :=rpcCall(worker,"Worker.DoTask",arg,new(struct{}))
		// fmt.Printf("finished %s\n",ok);
		if ok {
			jobFinishChan <- true
			mr.registerChannel <- worker
		} else {
			fmt.Printf("Failed task %d\n", arg.taskIdx)
			failedJob <- arg.taskIdx
		}
	}(worker,arg)
}
// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase string) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
		case mapJob:
			ntasks = len(mr.files)
			nios = mr.nReduce
		case reduceJob:
			ntasks = mr.nReduce
			nios = len(mr.files)
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	//
	jobFinishChan := make(chan bool,ntasks)
	failedJob := make(chan int,ntasks)
	// fmt.Printf("size %d\n",len(mr.workers))
	for i:=0; i < ntasks; i++{
		worker := <- mr.registerChannel
		arg := new(TaskArgs)
		arg.jobName = mr.job
		arg.file = mr.files[i]
		arg.phrase = phase
		arg.taskIdx = i
		arg.nios = nios
		mr.assignJob(worker,arg,jobFinishChan,failedJob)
	}
	go func(failedJob chan int){
		for i:= range failedJob{
			worker:=<-mr.registerChannel
			arg := new(TaskArgs)
			arg.jobName = mr.job
			arg.file = mr.files[i]
			arg.phrase = phase
			arg.taskIdx = i
			arg.nios = nios
			mr.assignJob(worker,arg,jobFinishChan,failedJob)
		}
	}(failedJob)	
	for i:=0; i < ntasks; i++{
		<- jobFinishChan
	}
	close(failedJob)
	fmt.Printf("Schedule: %v phase done\n", phase)
}