package mapreduce

import (
	"fmt"
	"net/rpc"
)

//argument used for register rpc call
//Alougth we can directly use string as the string, but use a * struct is recommended
//is offical document and more fexible to scale
type RegisterArgs struct{
	Addr string
}
//argument for shutdown reply which will return the number of tasks this worker has completed
type ShutdownReply struct{
	nTasks int
}
type TaskArgs struct{
	jobName string
	file    string
	phrase	string	
	//nio is the number of input or output files, for mapper this is the reduce number
	//for reducer this is the intermediate file number mapper generated
	nios 	int 
	taskIdx	int

}
/**
 * function used to implement the rpc call. addr is the address of the server,
 * method is the function name we want to call, args is the argument, and reply is 
 * is used to store the reply message 
 */
func rpcCall(addr string,method string, args interface{},
	 reply interface{})bool{
	conn,error := rpc.Dial("unix",addr)
	defer conn.Close()
	if error != nil{
		fmt.Println(error)
		return false
	}
	error = conn.Call(method,args,reply)
	if error == nil{
		return true;
	}else{
		fmt.Println(error)
		return false
	}
}