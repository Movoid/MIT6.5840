package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType uint8

const (
	MapTaskType TaskType = iota
	ReduceTaskType
	ExitType
)

type PullTaskArgs struct {
}

type PullTaskReply struct {
	TaskType                   TaskType
	TaskID                     int
	DispatchTimestamp          int
	MapInputFileWorkerRelaPath string // map task only
}

type RequestMapReduceConfigArgs struct {
}

type RequestMapReduceConfigReply struct {
	Config MapReduceConfig
}

type ReportTaskDoneArgs struct {
	TaskType          TaskType
	TaskID            int
	DispatchTimestamp int
	WorkspaceAbsPath  string // 向 master 返回 worker 的 workspace 绝对路径.
}

type ReportTaskDoneReply struct {
}
