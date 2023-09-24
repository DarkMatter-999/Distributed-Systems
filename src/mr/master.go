package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type JobState int

const (
	Map JobState = iota
	Reduce
	Done
)

type MasterCoordinator struct {
	State       JobState
	NReduce     int
	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask
	MapTaskIds  map[int]struct{}
	MaxTask     int
	Lock        sync.Mutex
	WorkerCount int
	ExitedCount int
}

const TIMEOUT = 10 * time.Second

func StartMaster(files []string, nReduce int) *MasterCoordinator {
	m := MasterCoordinator{
		NReduce:    nReduce,
		MaxTask:    0,
		MapTaskIds: make(map[int]struct{}),
	}

	for _, f := range files {
		m.MapTasks = append(m.MapTasks, &MapTask{TaskDetails: TaskDetails{State: Pending}, Filename: f})
	}

	for i := 0; i < nReduce; i++ {
		m.ReduceTasks = append(m.ReduceTasks, &ReduceTask{TaskDetails: TaskDetails{State: Pending, Id: i}})
	}

	m.State = JobState(Pending)

	m.server()
	return &m
}

func (c *MasterCoordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1337")
	if e != nil {
		log.Fatal("Could open port for listening")
	}
	go http.Serve(l, nil)
}

func (c *MasterCoordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	return c.State == Done
}

func (c *MasterCoordinator) HeartBeat(_ *Empty, _ *Empty) error {
	return nil
}

func (c *MasterCoordinator) RequestTask(_ *Empty, reply *Task) error {
	reply.Operation = ToWait

	if c.State == Map {
		for _, task := range c.MapTasks {
			start := time.Now()

			c.Lock.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(start) {
				task.State = Pending
			}
			if task.State == Pending {
				task.StartTime = start
				task.State = Executing
				c.MaxTask++
				task.Id = c.MaxTask
				c.Lock.Unlock()
				log.Printf("Map task assigned %d %s", task.Id, task.Filename)

				reply.Operation = ToRun
				reply.IsMap = true
				reply.NReduce = c.NReduce
				reply.Mapfunc = *task
				return nil
			}
			c.Lock.Unlock()

		}
	} else if c.State == Reduce {
		for _, task := range c.ReduceTasks {
			start := time.Now()
			c.Lock.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(start) {
				task.State = Pending
			}
			if task.State == Pending {
				task.StartTime = start
				task.State = Executing
				task.IntermediateFilenames = nil
				for id := range c.MapTaskIds {
					task.IntermediateFilenames = append(task.IntermediateFilenames, fmt.Sprintf("mr-tmp-%d-%d", id, task.Id))
				}
				c.Lock.Unlock()
				log.Printf("Reduce task assigned %d", task.Id)

				reply.Operation = ToRun
				reply.IsMap = false
				reply.NReduce = c.NReduce
				reply.Reducefunc = *task

				return nil
			}
			c.Lock.Unlock()
		}
	}

	return nil
}

func (c *MasterCoordinator) Finish(args *FinishArgs, _ *Empty) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if args.MapDone {
		for _, task := range c.MapTasks {
			if task.Id == args.Id {
				task.State = Finished
				log.Printf("Finished task %d, total %d", task.Id, len(c.MapTasks))
				c.MapTaskIds[task.Id] = struct{}{}
				break
			}
		}

		for _, t := range c.MapTasks {
			if t.State != Finished {
				return nil
			}
		}
		c.State = Reduce
	} else {
		for _, task := range c.ReduceTasks {
			if task.Id == args.Id {
				task.State = Finished
				break
			}
		}
		for _, t := range c.ReduceTasks {
			if t.State != Finished {
				return nil
			}
		}
		c.State = Done
	}
	return nil
}
