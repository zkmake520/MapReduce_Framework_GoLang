package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

//this is rpc function call by master when all tasks are completed
func (worker * Worker) Shutdown(_*struct{},reply *ShutdownReply)error{
	worker.Lock()
	defer worker.Unlock()
	reply.nTasks = worker.nTasks
	worker.nRPC = 1
	worker.nTasks--
	return nil
}

//master use rpc to call this function and let worker to run task
func (worker * Worker) ExecuteTask(args *TaskArgs, _*struct{})error{
	fmt.Println("%s begin to execute task #%d on file %s \n",
		worker.id,args.taskIdx,args.file)
	if args.phrase == mapJob{
		doMap(args.jobName,args.taskIdx,args.file,args.nio,worker.Mapper)
	}else{
		doReduce(args.jobName,args.taskIdx,args.nio,worker.Reducer)	
	}
	fmt.Println("%s finished task #%d\n",args.taskIdx)
	return nil
}