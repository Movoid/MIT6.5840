package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type MapReduceConfig struct {
	TimeoutThreashold      time.Duration
	NMap                   int
	NReduce                int
	MasterWorkspaceAbsPath string
}

type TaskState uint8

const (
	IsPending TaskState = iota
	IsRunning
	IsDone
)

type MapTask struct {
	state              TaskState
	startTime          time.Time
	dispatchTimestamp  int
	fileWorkerRelaPath string
}

type ReduceTask struct {
	state             TaskState
	startTime         time.Time
	dispatchTimestamp int
	processId         int
}

type CoordinatorState uint8

const (
	MapPhase CoordinatorState = iota
	ReducePhase
	AllDonePhase
)

// Master 数据结构
type Coordinator struct {
	// Your definitions here.
	config MapReduceConfig

	mapTaskTableMtx sync.Mutex
	mapTaskTable    []MapTask

	reduceTaskTableMtx sync.Mutex
	reduceTaskTable    []ReduceTask

	pendingMapTask    chan int
	pendingReduceTask chan int

	nextDispatchId int

	stateMtx sync.Mutex
	state    CoordinatorState
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestMapReduceConfig(_ *RequestMapReduceConfigArgs, reply *RequestMapReduceConfigReply) error {
	reply.Config = c.config
	return nil
}

func (c *Coordinator) PullTask(_ *PullTaskArgs, reply *PullTaskReply) error {
	for taskID := range c.pendingMapTask {
		c.mapTaskTableMtx.Lock()
		task := &c.mapTaskTable[taskID]
		if task.state != IsPending {
			c.mapTaskTableMtx.Unlock()
			continue
		}

		task.state = IsRunning
		task.startTime = time.Now()
		task.dispatchTimestamp = c.nextDispatchId
		c.nextDispatchId++

		reply.TaskType = MapTaskType
		reply.TaskID = taskID
		reply.DispatchTimestamp = task.dispatchTimestamp
		reply.MapInputFileWorkerRelaPath = task.fileWorkerRelaPath
		c.mapTaskTableMtx.Unlock()
		return nil
	}

	for taskID := range c.pendingReduceTask {
		c.reduceTaskTableMtx.Lock()
		task := &c.reduceTaskTable[taskID]
		if task.state != IsPending {
			c.reduceTaskTableMtx.Unlock()
			continue
		}

		task.state = IsRunning
		task.startTime = time.Now()
		task.dispatchTimestamp = c.nextDispatchId
		c.nextDispatchId++

		reply.TaskType = ReduceTaskType
		reply.TaskID = taskID
		reply.DispatchTimestamp = task.dispatchTimestamp
		reply.MapInputFileWorkerRelaPath = ""
		c.reduceTaskTableMtx.Unlock()
		return nil
	}

	// 两个 Channel 都被 `close` .
	reply.TaskType = ExitType
	return nil
}

/*
Master 作为协调者, 负责将 Worker 的临时文件提交为最终文件.
Map Worker 在 `worker-<pid>-map-<taskid>` 下工作, 生成 `part-<partid>` 文件.
Reduce Worker 在 `worker-<pid>-reduce-<taskid>` 下工作，生成 `out` 文件.

如果一个 Map Worker 成功完成 Map Task,
Master 将其 workspace 直接 `rename` 为 `map-<taskid>` .

所有 Map Task 完成后，
Reduce Worker 固定从 Master CWD 下的 `map-<taskid>` 下读取固定 `part-<partid>` 作为输入.

如果一个 Reduce Worker 成功完成 Reduce Task,
Master 将其 `out` 文件 `rename` 到 Master 工作目录下的 `mr-out-<taskid>` .
*/
func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, _ *ReportTaskDoneReply) error {
	commitMapTask := func(args *ReportTaskDoneArgs) error {
		if filepath.IsAbs(args.WorkspaceAbsPath) == false {
			return fmt.Errorf("worker workspace %v path is not abs.", args.WorkspaceAbsPath)
		}

		commitDirRelaPath := fmt.Sprintf("map-%d", args.TaskID)

		if err := os.RemoveAll(commitDirRelaPath); err != nil {
			return fmt.Errorf("cannot remove old map task commit dir %v: %v", commitDirRelaPath, err)
		}

		if err := os.Rename(args.WorkspaceAbsPath, commitDirRelaPath); err != nil {
			return fmt.Errorf("cannot rename %v to %v: %v", args.WorkspaceAbsPath, commitDirRelaPath, err)
		}

		return nil
	}

	commitReduceTask := func(args *ReportTaskDoneArgs) error {
		if filepath.IsAbs(args.WorkspaceAbsPath) == false {
			return fmt.Errorf("worker workspace %v path is not abs.", args.WorkspaceAbsPath)
		}

		workerOutputFileAbsPath := filepath.Join(args.WorkspaceAbsPath, "out")

		fuckTestDirAbsPath := filepath.Dir(args.WorkspaceAbsPath)

		commitFileAbsPath := filepath.Join(
			fuckTestDirAbsPath,
			fmt.Sprintf("mr-out-%d", args.TaskID),
		)

		if err := os.Rename(workerOutputFileAbsPath, commitFileAbsPath); err != nil {
			return fmt.Errorf("cannot rename %v to %v: %v", workerOutputFileAbsPath, commitFileAbsPath, err)
		}

		return nil
	}

	switch args.TaskType {
	case MapTaskType:
		if args.TaskID < 0 || args.TaskID >= len(c.mapTaskTable) {
			return fmt.Errorf("invalid map task ID %d", args.TaskID)
		}

		c.mapTaskTableMtx.Lock()
		defer c.mapTaskTableMtx.Unlock()
		task := &c.mapTaskTable[args.TaskID]
		if task.state != IsRunning {
			return nil
		}
		if task.dispatchTimestamp != args.DispatchTimestamp {
			return nil
		}

		// master 将此 worker 的临时文件提升为 map task 正式结果
		if err := commitMapTask(args); err != nil {
			return err
		}
		task.state = IsDone
		return nil

	case ReduceTaskType:
		if args.TaskID < 0 || args.TaskID >= len(c.reduceTaskTable) {
			return fmt.Errorf("invalid reduce task ID %d", args.TaskID)
		}

		c.reduceTaskTableMtx.Lock()
		defer c.reduceTaskTableMtx.Unlock()
		task := &c.reduceTaskTable[args.TaskID]
		if task.state != IsRunning {
			return nil
		}
		if task.dispatchTimestamp != args.DispatchTimestamp {
			return nil
		}

		if err := commitReduceTask(args); err != nil {
			return err
		}
		task.state = IsDone
		return nil

	default:
		return fmt.Errorf("invalid task type %d", args.TaskType)
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.stateMtx.Lock()
	defer c.stateMtx.Unlock()
	return c.state == AllDonePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("master cannot get cwd: %v", err)
	}

	c.config = MapReduceConfig{
		TimeoutThreashold:      time.Second * 10,
		NMap:                   len(files),
		NReduce:                nReduce,
		MasterWorkspaceAbsPath: cwd,
	}

	c.mapTaskTable = make([]MapTask, len(files))
	c.pendingMapTask = make(chan int, len(files))
	for i := range c.mapTaskTable {
		c.mapTaskTable[i] = MapTask{
			state:              IsPending,
			fileWorkerRelaPath: files[i],
		}
		c.pendingMapTask <- i
	}

	c.reduceTaskTable = make([]ReduceTask, nReduce)
	c.pendingReduceTask = make(chan int, nReduce)

	// 0 is invalid.
	c.nextDispatchId++

	c.server(sockname)

	go func() {
		ticker := time.NewTicker(time.Second * 1)

	TickerLoop:
		for range ticker.C {
			c.stateMtx.Lock()
			curState := c.state
			c.stateMtx.Unlock()

			switch curState {
			case MapPhase:

				allMapTaskIsDone := true
				c.mapTaskTableMtx.Lock()
				for i := range c.mapTaskTable {
					curTask := &c.mapTaskTable[i]
					if curTask.state != IsDone {
						allMapTaskIsDone = false
					}
					if curTask.state == IsRunning &&
						time.Since(curTask.startTime) > c.config.TimeoutThreashold {
						curTask.state = IsPending
						curTask.dispatchTimestamp = 0
						c.pendingMapTask <- i
					}
				}
				c.mapTaskTableMtx.Unlock()

				if allMapTaskIsDone {
					transferredState := false
					c.stateMtx.Lock()
					if c.state == MapPhase {
						c.state = ReducePhase
						transferredState = true
					}
					c.stateMtx.Unlock()
					if transferredState {
						close(c.pendingMapTask)
						for i := range c.reduceTaskTable {
							c.reduceTaskTable[i] = ReduceTask{
								state:     IsPending,
								processId: i,
							}
							c.pendingReduceTask <- i
						}
					}
				}

			case ReducePhase:

				allReduceTaskIsDone := true
				c.reduceTaskTableMtx.Lock()
				for i := range c.reduceTaskTable {
					curTask := &c.reduceTaskTable[i]
					if curTask.state != IsDone {
						allReduceTaskIsDone = false
					}

					if curTask.state == IsRunning &&
						time.Since(curTask.startTime) > c.config.TimeoutThreashold {
						curTask.state = IsPending
						curTask.dispatchTimestamp = 0
						c.pendingReduceTask <- i
					}
				}
				c.reduceTaskTableMtx.Unlock()

				if allReduceTaskIsDone {
					transferredState := false
					c.stateMtx.Lock()
					if c.state == ReducePhase {
						c.state = AllDonePhase
						transferredState = true
					}
					c.stateMtx.Unlock()
					if transferredState {
						close(c.pendingReduceTask)
					}
					break TickerLoop
				}

			case AllDonePhase:
				break TickerLoop
			}
		}
		ticker.Stop()
	}()

	return &c
}
