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
