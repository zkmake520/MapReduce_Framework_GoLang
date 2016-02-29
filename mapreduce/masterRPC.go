package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)







// master node start the server for listening to RPC calls from workers
func (master *Master) startRPCServer(){
	server := rpc.NewServer()
	server.register(master)
	os.Remove(mr.addr)
	listener, error = net.Listen("unix",mr.addr)
	if error != nil{
		log.Fatal("Server registration failed",mr.addr," error:",error)
	}
	master.listener = listener
	//Create new thread to listen on the master addr
	go func(){
		for {
			select{
				case <-master.shutdown:
					break //TODO?
				default:
			}
			connection, error = master.listener.Accept()
			if error == nil{
				//create a new thread to serve this rpc call,otherwise may block
				go func(){
					server.ServeConn(connection)
					connection.Close()
				}
			}else{
				fmt.Printf("Server error:accept error ",error)
			}

		}	
	}()
}