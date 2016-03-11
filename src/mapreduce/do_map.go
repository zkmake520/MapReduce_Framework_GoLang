package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"encoding/json"
)

func doMap(jobName string, mapTaskNumber int,inFile string,
	nReduce int, mapF func(doc string,content string)([]KeyValue)){
	contents,error := ioutil.ReadFile(inFile)
	if error != nil{
		log.Fatal("Job ",jobName,": task#",mapTaskNumber," read file failed. error ",error)
		return 
	}
	pairs := mapF(inFile,string(contents))
	reduceFiles := make([]*os.File,nReduce,nReduce)
	encoder := make([]*json.Encoder,nReduce,nReduce)
	for i:=0;i < nReduce; i++{
		reduceFilesName := reduceName(jobName, mapTaskNumber,i)
		reduceFile,error:= os.Create(reduceFilesName)
		reduceFiles[i] = reduceFile
		if error !=nil{
			log.Fatal("Job ",jobName,": task#",mapTaskNumber," open file failed. error ",error)
		}
		encoder[i] = json.NewEncoder(reduceFiles[i])
	}
	for _,kv:= range pairs{
		reducer := ihash(kv.Key) % uint32(nReduce)
		error:=encoder[reducer].Encode(&kv)
		if error!=nil{
			log.Fatal("Key ",kv.Key," outputs failed")
		}
	}
	for i:=0; i < nReduce; i++{
		reduceFiles[i].Close()
	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
