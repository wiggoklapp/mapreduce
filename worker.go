package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// type WorkerStruct struct {
// 	WorkerID string
// }

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function. Create the worker
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := GetTask()

		if reply.TaskType { // IF MAPTASK
			ExecMapTask(reply.MapTask.FileName, reply.MapTask.TaskNumber, reply.NReduce, mapf)
			TaskFinished(reply.MapTask.TaskNumber, true)
		} else { // IF REDUCETASK
			ExecReduceTask(reply.ReduceTask.TaskNumber, reducef)
			TaskFinished(reply.ReduceTask.TaskNumber, false)
		}

	}
}

// RPC call to the coordinator from worker, asking for a task.
func GetTask() *TaskReply {
	taskrequest := TaskRequest{}
	taskreply := TaskReply{}
	ok := call("Coordinator.AssignTask", &taskrequest, &taskreply)
	if ok {
		fmt.Printf("FileName is: %v\n", taskreply.MapTask.FileName)
		return &taskreply
	} else {
		fmt.Printf("GetTask call failed!\n")
		return nil
	}
}

func ExecMapTask(filename string, maptasknumber int, nreduce int, mapf func(string, string) []KeyValue) {
	// Open and read the file given to the worker
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// Map the file to the intermedate list, then sort it
	intermediate := mapf(filename, string(content))
	sort.Sort(ByKey(intermediate))

	// Create a list of
	listOfLists := make([][]KeyValue, nreduce)
	for _, keyval := range intermediate {
		hashVal := ihash(keyval.Key) % nreduce
		listOfLists[hashVal] = append(listOfLists[hashVal], keyval)
	}

	for i := 0; i < nreduce; i++ {
		newFileName := "mr-" + strconv.Itoa(maptasknumber-1) + "-" + strconv.Itoa((i))
		newFile, err := os.Create(newFileName)
		if err != nil {
			log.Fatalf("cannot create %v", newFileName)
		}
		defer newFile.Close()

		_, err = os.Open(newFileName)
		if err != nil {
			log.Fatalf("cannot open %v", newFileName)
		}
		enc := json.NewEncoder(newFile)
		for _, kv := range listOfLists[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode to %v", newFile)
			}
		}
	}
}

// Checks the current dir and returns a list of files to be reduced
func FilesToReduce(reducetasknumber int) ([]string, error) {
	var filestoreduce []string
	dir := "./"
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {

		matched, err := regexp.MatchString(fmt.Sprintf(`mr-\d-%d`, reducetasknumber), path)
		if err != nil {
			return err
		}
		if matched {
			filestoreduce = append(filestoreduce, path)
			return nil
		}

		return nil
	})
	return filestoreduce, err
}

func ExecReduceTask(reducetasknumber int, reducef func(string, []string) string) {

	kva := make([]KeyValue, 0)

	// Find all 8 files that end with the specificed reducetasknumber
	filenames, _ := FilesToReduce(reducetasknumber)

	// Go through the files and
	for _, file := range filenames {
		file, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

	}

	// Create a new file for the output of the reduce task
	outputFileName := "mr-out-" + strconv.Itoa((reducetasknumber))
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("cannot create %v", outputFileName)
	}
	defer outputFile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

}

func TaskFinished(tasknumber int, tasktype bool) error {
	taskfinishedrequest := TaskFinishedRequest{
		TaskNumber: tasknumber,
		TaskType:   tasktype,
	}
	taskfinishedreply := TaskFinishedReply{}

	ok := call("Coordinator.TaskFinished", &taskfinishedrequest, &taskfinishedreply)
	if ok {
		return nil
	} else {
		fmt.Printf("TaskFinished call failed!\n")
		return nil
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
