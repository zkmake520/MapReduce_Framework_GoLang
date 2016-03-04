package mapreduce

import "fmt"

//schedule tasks to workers
func (master *Master) schedule(){
	var nTasks int
	if(master.phrase = mapJob){
		nTasks = len(master.files)
	}else{
		nTasks = master.nReduce
	}
	fmt.Printf("Schedule: %v %v tasks \n", ntasks, master.phrase)
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Workers may fail, and that any given worker may finish
	// multiple tasks.
	


}