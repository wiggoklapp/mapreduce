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

const (
	IDLE     = 0
	RUNNING  = 1
	FINISHED = 2
)

type Coordinator struct {
	MapTasks             []*MapTask
	ReduceTasks          []*ReduceTask
	NReduce              int
	RemainingMapTasks    int
	RemainingReduceTasks int
	Mu                   *sync.Mutex
}

type MapTask struct {
	State        int
	TimeAccepted time.Time
	FileName     string
	TaskNumber   int
}

type ReduceTask struct {
	State        int
	TimeAccepted time.Time
	FileName     string
	TaskNumber   int
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:             make([]*MapTask, len(files)),
		ReduceTasks:          make([]*ReduceTask, nReduce),
		NReduce:              nReduce,
		RemainingMapTasks:    len(files) - 1,
		RemainingReduceTasks: nReduce,
	}

	// Make the MapTasks
	for i, file := range files {
		c.MapTasks[i] = &MapTask{
			State:      IDLE,
			FileName:   file,
			TaskNumber: i,
		}
	}

	// Make the ReduceTasks
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = &ReduceTask{
			State:      IDLE,
			TaskNumber: i,
		}

	}
	c.server()
	return &c
}

// RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignTask(taskrequest *TaskRequest, taskreply *TaskReply) error {

	c.Mu.Lock()
	defer c.Mu.Unlock()
	// Map tasks available to give to workers
	if c.RemainingMapTasks > 0 {
		for _, maptask := range c.MapTasks {
			if maptask.State == IDLE && maptask.FileName != "wc.so" {
				mt := TaskReply{
					MapTask: MapTask{
						State:        RUNNING,
						TimeAccepted: time.Now(),
						FileName:     maptask.FileName,
						TaskNumber:   maptask.TaskNumber,
					},
					NReduce:  c.NReduce,
					TaskType: true,
				}
				*taskreply = mt
				break
			}
		}
		return nil

	}

	// Reduce tasks avaliable to give to workers
	if c.RemainingReduceTasks > 0 {
		for _, reducetask := range c.ReduceTasks {
			if reducetask.State == IDLE {
				rt := TaskReply{
					ReduceTask: ReduceTask{
						State:        RUNNING,
						TimeAccepted: time.Now(),
						TaskNumber:   reducetask.TaskNumber,
					},
					TaskType: false,
				}
				*taskreply = rt
				break
			}
		}
		return nil
	}
	// No tasks available to give to workers
	fmt.Println("No tasks available")
	return nil
}

// Sets state of a task to finished and decrements remainingtasks
func (c *Coordinator) TaskFinished(taskfinishedrequest *TaskFinishedRequest, taskfinishedreply *TaskFinishedReply) error {
	if taskfinishedrequest.TaskType {
		c.MapTasks[taskfinishedrequest.TaskNumber].State = FINISHED
		c.RemainingMapTasks -= 1
		return nil
	} else {
		c.ReduceTasks[taskfinishedrequest.TaskNumber].State = FINISHED
		c.RemainingReduceTasks -= 1
		return nil
	}

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
	if c.RemainingMapTasks == 0 && c.RemainingReduceTasks == 0 {
		return true
	}
	return false

}
