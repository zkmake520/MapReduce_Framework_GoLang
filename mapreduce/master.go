package mapreduce


import (
	"fmt"
	"net"
	"sync"
)

type Master struct{
	m Sync.mutex

	addr 			string
	registerChannel chan string
	workers 	 	[] string
	listener		net.Listener
	files 			[]string
	finished        chan bool
}

//create a new master 
func createNewMaster(addr string) (master *Master){
	master = new(Master)
	master.addr = addr
	master.registerChannel = make(chan string)
	master.finished = make(chan bool)
	master.shutdown = make(chan struct{})
}
// Run the map/reduce task on a given master.
// Tasks for a master:
// 					1. start a rpc server and create new thread to listen on it
// 					2. create a new thread to start the master job, including schedule the each task
// 						one workers as they become available. Once all mappers have finished, master will schedule
// 						all workers to do the reduce jobs.Once all taks have been done, the reducer outputs will be collected 
// 						and merged. Finally the rpc server will be stopped and workers will be killed
// 			
func masterRun(addr string,jobName string,files []string,nReduce int) (master *Master){
	master = createNewMaster(addr)
	master.startRPCServer()
	go master.startWork(jobName,files,nReduce)	
}
func (master *Master) finish(){
	master.stopRPCServer()
	//TODO kill all worker server
	master.finished<-true
}
func (master *Master)startWork(jobName string, files []string, nReduce int){
	//set up basic job information
	master.jobName = jobName
	master.files = files
	master.nReduce = nReduce	

	//start to schedule jobs
	fmt.Printf("Starting map/reduce job:%s",jobName)
	master.schedule()	
	master.merge()

	//finish job
	fmt.Printf("Finishing map/reduce job:%s",jobName)
	master.finish()
}


