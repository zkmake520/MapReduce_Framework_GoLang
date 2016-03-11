package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)


//this a rpc method which will be called by workers to notify master that they have 
//been ready
func (master *Master) Register(args *RegisterArgs,_*struct{}) error{
	master.m.Lock()
	defer master.m.Unlock()
	fmt.Printf("Worker %s has registed\n",args.Addr)
	// use new thread to add to channel ,if the channel is full may block
	//it may delay the response speed, thus we'd better use a new thread
	master.workers = append(master.workers,args.Addr)
	go func(){
		master.registerChannel<-args.Addr 
	}()
	return nil

}
//Master call this function to terminate all workers
func (master * Master) terminateWorkers() {
	master.nTasks = make([]int , 0, len(master.workers))
	for _,w:= range master.workers{
		var reply ShutdownReply
		ok := rpcCall(w,"Worker.ShutDown",new(struct{}),&reply)
		if ok == false{
			fmt.Printf("Worker %s shutdown failed",w)
		}else{
			master.nTasks = append(master.nTasks,reply.nTasks)
		}
	}
	return
}

// master node start the server for listening to RPC calls from workers
func (master *Master) startRPCServer(){
	server := rpc.NewServer()
	server.Register(master)
	os.Remove(master.Addr)
	listener, error := net.Listen("unix",master.Addr)
	if error != nil{
		log.Fatal("Server registration failed",master.Addr," error:",error)
	}
	master.listener = listener
	//Create new thread to listen on the master addr
	go func(){
		for {
			select{
				// case <-master.shutdown:
					// break //TODO?
				default:
			}
			connection, error := master.listener.Accept()
			if error == nil{
				//create a new thread to serve this rpc call,otherwise may block
				go func(){
					server.ServeConn(connection)
					connection.Close()
				}()
			}else{
				fmt.Printf("Server error:accept error ",error)
			}

		}	
	}()
}
// stop rpc server when jobs have been completed
func (master *Master) stopRPCServer(){
	//TODO: how to stop the rpc server without race condition
	//since the server thread is different with current thread
	//Solution:send a shutdown rpc call to server, then let server thread close the server by itself
}