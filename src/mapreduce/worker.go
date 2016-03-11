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
	fmt.Printf("Worker %s started\n",workerId)
	listener, error:= net.Listen("unix",workerId)
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
		worker.m.Unlock()
		connection,error:=worker.listener.Accept()
		if error != nil{
			break;
		}
		worker.m.Lock()
		worker.nRPC--
		worker.m.Unlock()
		//TODO: close connection or not? 
		go server.ServeConn(connection)	
		worker.m.Lock()
		worker.nTasks++
		worker.m.Unlock()
	}
	worker.listener.Close()
	fmt.Printf("Worker %s exited\n",workerId)
}