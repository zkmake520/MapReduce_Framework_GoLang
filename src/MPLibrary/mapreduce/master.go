package mapreduce


import (
	"fmt"
	"net"
	"sync"
)

type Master struct{
	m sync.Mutex

	addr 			string
	registerChannel chan string
	job             string
	workers 	 	[] string
	listener		net.Listener
	files 			[]string
	finished        chan bool
	nReduce			int
}

//create a new master 
func createNewMaster(addr string) (master *Master){
	master = new(Master)
	master.addr = addr
	master.registerChannel = make(chan string)
	master.finished = make(chan bool)
	// master.shutdown = make(chan struct{})
	return 
}
// Run the map/reduce task on a given master.
// Tasks for a master:
// 					1. start a rpc server and create new thread to listen on it
// 					2. create a new thread to start the master job, including schedule the each task
// 						one workers as they become available. Once all mappers have finished, master will schedule
// 						all workers to do the reduce jobs.Once all taks have been done, the reducer outputs will be collected 
// 						and merged. Finally the rpc server will be stopped and workers will be killed
// 			
func masterRun(addr string,jobName string,files []string,nReduce int) {
	master := createNewMaster(addr)
	master.startRPCServer()
	go master.startWork(jobName,files,nReduce)	
}

//clean up routinue for master
func (master *Master) finish(){
	master.stopRPCServer()
	//TODO kill all worker server
	master.finished<-true
}


// master will schedule tasks and merge results
func (master *Master)startWork(job string, files []string, nReduce int){
	//set up basic job information
	master.job = job
	master.files = files
	master.nReduce = nReduce	

	//start to schedule jobs
	fmt.Printf("Starting map/reduce job:%s",job)
	// master.schedule()	
	// master.merge()

	//finish job
	fmt.Printf("Finishing map/reduce job:%s",job)
	master.finish()
}

