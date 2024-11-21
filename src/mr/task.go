package mr

import "time"

type Task struct {
	TaskId           int
	Status           int
	TaskType         int
	Filename         string
	StartRunningTime time.Time

	// map
	NReduce            int
	StoredPartitionIds []int

	// reduce
	PartitionId int
	MapTaskIds  []int
}

var (
	TASK_TYPE_MAP    = 0
	TASK_TYPE_REDUCE = 1
	TASK_TYPE_NONE   = 2

	TASK_STATUS_READY   = 0
	TASK_STATUS_RUNNING = 1
	TASK_STATUS_DONE    = 2
	TASK_STATUS_FAIL    = 3
)
