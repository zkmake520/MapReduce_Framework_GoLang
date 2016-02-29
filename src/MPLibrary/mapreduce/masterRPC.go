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
func (master *Master) register(args *RegisterArgs,_*struct{}) error{
	master.m.Lock()
	defer master.m.Unlock()
	fmt.Printf("Worker %s has registed\n",args.addr)
	// use new thread to add to channel ? if the channel is full may block?
	//it may delay the response speed, thus we'd better use a new thread
	go func(){
		master.registerChannel<-args.addr 
	}()
	return nil

}



// master node start the server for listening to RPC calls from workers
func (master *Master) startRPCServer(){
	server := rpc.NewServer()
	server.Register(master)
	os.Remove(master.addr)
	listener, error := net.Listen("unix",master.addr)
	if error != nil{
		log.Fatal("Server registration failed",master.addr," error:",error)
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

}