/*
 * taskqueue.go
 * A simple task queue implementation with a linked list.
 */

package mr

type node struct {
	Task *Task
	Next *node
}

type TaskQueue struct {
	Head *node
	Tail *node
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{Head: nil}
}

func (q *TaskQueue) Push(task *Task) {
	newNode := &node{Task: task, Next: nil}
	if q.IsEmpty() {
		q.Head = newNode
		q.Tail = newNode
		return
	}

	q.Tail.Next = newNode
	q.Tail = newNode
}

func (q *TaskQueue) Pop() *Task {
	if q.IsEmpty() {
		return nil
	}

	popNode := q.Head
	q.Head = popNode.Next

	if q.Head == nil {
		q.Tail = nil
	}

	return popNode.Task
}

func (q *TaskQueue) Front() *Task {
	if q.IsEmpty() {
		return nil
	}

	return q.Head.Task
}

func (q *TaskQueue) IsEmpty() bool {
	return q.Head == nil
}
