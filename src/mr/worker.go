package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

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

func getTask() (Task, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTaskHandler", &args, &reply)
	if !ok {
		return Task{}, fmt.Errorf("call failed")
	}

	return reply.Task, nil
}

/*
 * createAndOpenFile open the file with provided path. If the parent directory or the file is not been create, the function will create them.
 */
func createAndOpenFile(filePath string) (*os.File, error) {
	// create parent directory if not exist
	parentDir := filepath.Dir(filePath)
	if _, err := os.Stat(parentDir); os.IsNotExist(err) {
		os.MkdirAll(parentDir, 0755)
	}

	// create file if not exist
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func doMap(task *Task, mapf func(string, string) []KeyValue) error {
	data, err := os.ReadFile(task.Filename)
	if err != nil {
		return fmt.Errorf("cannot open %v", task.Filename)
	}

	kva := mapf(task.Filename, string(data))

	// put kva in difference partition
	kvMap := make(map[int][]KeyValue)
	for _, kv := range kva {
		partitionId := ihash(kv.Key) % task.NReduce
		kvMap[partitionId] = append(kvMap[partitionId], kv)
	}

	// store kv in temporary file group by different partition
	for partitionId, kvArr := range kvMap {
		if len(kvArr) == 0 {
			continue
		}

		tmpFilePath := fmt.Sprintf("tmp/intermediate-%d-%d.json", partitionId, task.TaskId)

		file, err := createAndOpenFile(tmpFilePath)
		if err != nil {
			return fmt.Errorf("cannot create file %v", tmpFilePath)
		}

		intermediate := []KeyValue{}
		intermediate = append(intermediate, kvArr...)

		jsonContent, err := json.Marshal(intermediate)
		if err != nil {
			return fmt.Errorf("cannot marshal intermediate %v", intermediate)
		}

		fmt.Fprintf(file, "%v", string(jsonContent))

		task.StoredPartitionIds = append(task.StoredPartitionIds, partitionId)

		file.Close()
	}

	return nil
}

func doReduce(task *Task, reducef func(string, []string) string) error {
	// load all intermediate files
	intermediate := make(map[string][]string)
	for _, mapTaskId := range task.MapTaskIds {
		tmpFilePath := fmt.Sprintf("tmp/intermediate-%d-%d.json", task.PartitionId, mapTaskId)

		data, err := os.ReadFile(tmpFilePath)
		if err != nil {
			return fmt.Errorf("cannot open %v", tmpFilePath)
		}

		// unmarshal json
		var kva []KeyValue
		json.Unmarshal(data, &kva)

		for _, kv := range kva {
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}
	}

	// create output file
	outputFilePath := fmt.Sprintf("mr-out-%d", task.PartitionId)
	file, err := createAndOpenFile(outputFilePath)
	if err != nil {
		return fmt.Errorf("cannot create file %v", outputFilePath)
	}

	for key, values := range intermediate {
		output := reducef(key, values)
		fmt.Fprintf(file, "%v %v\n", key, output)
	}

	file.Close()

	return nil
}

func callTaskDone(task Task) error {
	args := &TaskDoneArgs{Task: task}
	reply := &TaskDoneReply{}

	ok := call("Coordinator.TaskDoneHandler", args, reply)
	if !ok {
		return fmt.Errorf("call failed")
	}

	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Printf("Worker Start Running...\n")

	for {
		task, err := getTask()

		if err != nil || task.TaskType == TASK_TYPE_NONE {
			time.Sleep(time.Second)
			continue
		}

		if task.TaskType == TASK_TYPE_MAP {
			err := doMap(&task, mapf)
			if err != nil {
				task.Status = TASK_STATUS_FAIL

				callTaskDone(task)
				continue
			}
		} else if task.TaskType == TASK_TYPE_REDUCE {
			err := doReduce(&task, reducef)
			if err != nil {
				task.Status = TASK_STATUS_FAIL

				callTaskDone(task)
				continue
			}
		}

		task.Status = TASK_STATUS_DONE
		callTaskDone(task)
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
