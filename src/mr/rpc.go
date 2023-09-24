package mr

import "time"

type FinishArgs struct {
	MapDone bool
	Id      int
}

type TaskState int

type Empty struct{}

const (
	Pending TaskState = iota
	Executing
	Finished
)

type TaskDetails struct {
	State     TaskState
	StartTime time.Time
	Id        int
}

type TaskOperation int

const (
	ToWait TaskOperation = iota
	ToRun
)

type MapTask struct {
	TaskDetails
	Filename string
}

type ReduceTask struct {
	TaskDetails
	IntermediateFilenames []string
}

type Task struct {
	Operation  TaskOperation
	IsMap      bool
	NReduce    int
	Mapfunc    MapTask
	Reducefunc ReduceTask
}
