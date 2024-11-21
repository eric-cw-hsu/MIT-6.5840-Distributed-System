package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	COORDINATOR_STATUS_MAP    = 0
	COORDINATOR_STATUS_REDUCE = 1
)

type Coordinator struct {
	CoordinatorStatus    int
	ReadyTaskQueue       TaskQueue
	RunningTaskContainer map[int]*Task
	TaskStatusLock       sync.Mutex
	TaskCount            int
	NReduce              int
	PartitionTaskMap     map[int][]int
}

func (c *Coordinator) init(nReduce int) {
	c.TaskCount = 0
	c.RunningTaskContainer = make(map[int]*Task)
	c.CoordinatorStatus = COORDINATOR_STATUS_MAP
	c.NReduce = nReduce
	c.PartitionTaskMap = make(map[int][]int)
	c.ReadyTaskQueue = TaskQueue{}

	c.clearIntermediateFiles()
}

func (c *Coordinator) clearIntermediateFiles() {
	os.RemoveAll("tmp")
}

func (c *Coordinator) getNewTaskId() int {
	id := c.TaskCount
	c.TaskCount += 1

	return id
}

func (c *Coordinator) loadMapTasks(files []string) {
	for _, file := range files {
		c.ReadyTaskQueue.Push(&Task{
			TaskId:             c.getNewTaskId(),
			TaskType:           TASK_TYPE_MAP,
			Filename:           file,
			NReduce:            c.NReduce,
			StoredPartitionIds: []int{},
		})
	}
}

func (c *Coordinator) loadReduceTasks() {
	for partitionId, taskIds := range c.PartitionTaskMap {
		task := &Task{
			TaskId:      c.getNewTaskId(),
			TaskType:    TASK_TYPE_REDUCE,
			PartitionId: partitionId,
			MapTaskIds:  taskIds,
		}

		c.ReadyTaskQueue.Push(task)
	}
}

func (c *Coordinator) GetTaskHandler(args *GetTaskArgs, reply *GetTaskReply) error {
	c.TaskStatusLock.Lock()
	defer c.TaskStatusLock.Unlock()

	if c.ReadyTaskQueue.IsEmpty() {
		reply.Task = Task{TaskType: TASK_TYPE_NONE}
	} else {
		task := c.ReadyTaskQueue.Pop()

		task.Status = TASK_STATUS_RUNNING
		task.StartRunningTime = time.Now()

		reply.Task = *task
		c.RunningTaskContainer[task.TaskId] = task
	}
	return nil
}

func (c *Coordinator) launchOrphanedTaskKiller() {
	go func() {
		for {
			time.Sleep(1 * time.Second)

			c.TaskStatusLock.Lock()
			for _, task := range c.RunningTaskContainer {
				if time.Since(task.StartRunningTime) > 10*time.Second {
					fmt.Printf("Task %d is an orphaned task, put it back to the queue\n", task.TaskId)
					task.Status = TASK_STATUS_READY
					c.ReadyTaskQueue.Push(task)
					delete(c.RunningTaskContainer, task.TaskId)
				}
			}
			c.TaskStatusLock.Unlock()
		}
	}()
}

/*
 * The RPC Handler that handlers when the worker ends the task.
 * This function offers a TaskStatusLock to avoid the concurrency problem when multi worker try to end their task
 */
func (c *Coordinator) TaskDoneHandler(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.TaskStatusLock.Lock()
	defer c.TaskStatusLock.Unlock()

	doneTask := args.Task

	// remove from running task tracker
	delete(c.RunningTaskContainer, doneTask.TaskId)

	// put the task back to ready queue if the job fails
	if doneTask.Status == TASK_STATUS_FAIL {
		doneTask.Status = TASK_STATUS_READY
		c.ReadyTaskQueue.Push(&doneTask)

		return nil
	}

	if doneTask.TaskType == TASK_TYPE_MAP {
		for _, partitionId := range doneTask.StoredPartitionIds {
			if _, ok := c.PartitionTaskMap[partitionId]; !ok {
				c.PartitionTaskMap[partitionId] = []int{}
			}

			c.PartitionTaskMap[partitionId] = append(c.PartitionTaskMap[partitionId], doneTask.TaskId)
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.TaskStatusLock.Lock()
	defer c.TaskStatusLock.Unlock()

	if c.ReadyTaskQueue.IsEmpty() && len(c.RunningTaskContainer) == 0 {
		if c.CoordinatorStatus == COORDINATOR_STATUS_MAP {
			fmt.Println("--REDUCE MODE")
			c.CoordinatorStatus = COORDINATOR_STATUS_REDUCE
			c.loadReduceTasks()
		} else {
			ret = true
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	fmt.Println("Coordinator Start Running...")
	fmt.Println("--MAP MODE")
	c.init(nReduce)
	c.loadMapTasks(files)
	c.launchOrphanedTaskKiller()

	c.server()
	return &c
}
