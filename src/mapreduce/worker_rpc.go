package mapreduce

import (
	"fmt"
)

//this is rpc function call by master when all tasks are completed
func (worker * Worker) Shutdown(_*struct{},reply *ShutdownReply)error{
	worker.m.Lock()
	defer worker.m.Unlock()
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
		doMap(args.jobName,args.taskIdx,args.file,args.nios,worker.Mapper)
	}else{
		doReduce(args.jobName,args.taskIdx,args.nios,worker.Reducer)	
	}
	fmt.Println("%s finished task #%d\n",args.taskIdx)
	return nil
}

//worker call this function to register himself
func (worker *Worker)register(masterId string){
	args := new(RegisterArgs)
	args.Addr = worker.id
	ok := rpcCall(masterId,"Master.Register",args,new(struct{}))
	if ok == false{
		fmt.Printf("%s register error\n",worker.id)
	}
}