package mapreduce

import (
	"hash/fnv"
)

func doMap(jobName string, taskIdx string,file string,
	nReduce int, Mapper func(line string,string)([]KeyValue)){

}