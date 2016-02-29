package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type Worker struct{
	m sync.Mutex

	id 		string
	Mapper	func(string,string)([]KeyValue)	
	Reducer func(string,[]string) string
	nRPC	int
	nTasks	int
	listener net.Listener
}

//worker call this function to register himself
func (worker *Worker)register(masterId string){
	args := new(RegisterArgs)
	args.Worker = worker.id
	ok := rpcCall(masterId,"Master.Register",args,new(struct{]}))
	if ok == false{
		fmt.Printf("%s register error\n",worker.id)
	}
}
//WorkerRun will make a rpc call register to notify master that it is ready
//and wait for task
func WorkerRun(masterId string,workerId string,Mapper func(string,string)[] KeyValue,
				Reducer func(string,[]string)string,nRPC int){

	worker := new(Worker)
	worker.id = workerId
	worker.Mapper = Mapper
	worker.Reducer = Reducer
	worker.nRPC = nRPC
	server := rpc.NewServer()
	server.Register(worker)
	os.Remove(workerId)
	listener, error:= net.Listener("unix",workerId)
	if error!=nil{
		log.Fatal("WorkerRun:worker ",workerId," error ",error)

	}
	worker.listener = listener
	worker.register(masterId)
	for{
		worker.m.Lock()
		if(worker.nRPC == 0){
			worker.m.Unlock()
			break;
		}
		worker.Unlock()
		connection,error:=worker.listener.Accept()
		if errror != nil{
			break;
		}
		worker.Lock()
		worker.nRPC--
		worker.Unlock()
		//TODO: close connection or not? 
		go server.ServeConn(connection)	
		worker.Lock()
		worker.nTasks++
		worker.Unlock()
	}
	Worker.listener.Close()
	fmt.Println("Worker %s exited\n",workerId)
}