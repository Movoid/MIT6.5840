package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
var mapreduceConfig MapReduceConfig

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	mrConfig := RequestMapReduceConfigReply{}
	if ok := call("Coordinator.RequestMapReduceConfig", &RequestMapReduceConfigArgs{}, &mrConfig); ok == false {
		log.Fatal("cannot get mapreduce config.")
	}
	mapreduceConfig = mrConfig.Config
	// inited.

	for {

		task := PullTaskReply{}
		if ok := call("Coordinator.PullTask", &PullTaskArgs{}, &task); ok == false {
			log.Fatalf("worker %d cannot pull task.", os.Getpid())
		}

		switch task.TaskType {
		case MapTaskType:

			workspaceDirRelaPath := fmt.Sprintf("worker-%d-map-%d", os.Getpid(), task.TaskID)

			if err := os.RemoveAll(workspaceDirRelaPath); err != nil {
				log.Fatalf("worker %d cannot remove old workspace: %v", os.Getpid(), err)
			}

			if err := os.Mkdir(workspaceDirRelaPath, 0o755); err != nil {
				log.Fatalf("worker %d cannot mkdir workspace %v: %v", os.Getpid(), workspaceDirRelaPath, err)
			}

			inputFile, err := os.Open(task.MapInputFileWorkerRelaPath)
			if err != nil {
				log.Fatalf("worker %d cannot open file %v: %v", os.Getpid(), task.MapInputFileWorkerRelaPath, err)
			}

			content, err := io.ReadAll(inputFile)
			if err != nil {
				log.Fatalf("worker %d cannot read file %v: %v", os.Getpid(), inputFile, err)
			}
			inputFile.Close()

			outputFiles := make([](*os.File), mapreduceConfig.NReduce)
			for partID := range mapreduceConfig.NReduce {
				partFileRelaPath := fmt.Sprintf("part-%d", partID)
				partFile, err := os.Create(filepath.Join(workspaceDirRelaPath, partFileRelaPath))
				if err != nil {
					log.Fatalf("worker %d cannot create part file %v: %v", os.Getpid(), partFileRelaPath, err)
				}
				outputFiles[partID] = partFile
			}

			kva := mapf(task.MapInputFileWorkerRelaPath, string(content))

			for _, kv := range kva {
				keyHash := ihash(string(kv.Key)) % mapreduceConfig.NReduce
				fmt.Fprintf(outputFiles[keyHash], "%v %v\n", kv.Key, kv.Value)
			}

			for _, openFile := range outputFiles {
				openFile.Close()
			}

			workspaceAbsPath, _ := filepath.Abs(workspaceDirRelaPath)
			if ok := call("Coordinator.ReportTaskDone", &ReportTaskDoneArgs{
				TaskType:          MapTaskType,
				TaskID:            task.TaskID,
				DispatchTimestamp: task.DispatchTimestamp,
				WorkspaceAbsPath:  workspaceAbsPath,
			}, &ReportTaskDoneReply{}); ok == false {
				log.Fatalf("worker %d cannot report task done.", os.Getpid())
			}

		case ReduceTaskType:

			workspaceDirRelaPath := fmt.Sprintf("worker-%d-reduce-%d", os.Getpid(), task.TaskID)

			if err := os.RemoveAll(workspaceDirRelaPath); err != nil {
				log.Fatalf("worker %d cannot remove old workspace %v: %v", os.Getpid(), workspaceDirRelaPath, err)
			}

			if err := os.Mkdir(workspaceDirRelaPath, 0o755); err != nil {
				log.Fatalf("worker %d cannot mkdir workspace %v: %v", os.Getegid(), workspaceDirRelaPath, err)
			}

			outputFileRelaPath := filepath.Join(workspaceDirRelaPath, "out")

			outputFile, err := os.Create(outputFileRelaPath)
			if err != nil {
				log.Fatalf("worker %d cannot create reduce output file %v: %v", os.Getpid(), outputFileRelaPath, err)
			}

			allKV := make([]KeyValue, 0)

			for i := range mapreduceConfig.NMap {
				partFileAbsPath := filepath.Join(
					mapreduceConfig.MasterWorkspaceAbsPath,
					fmt.Sprintf("map-%d", i),
					fmt.Sprintf("part-%d", task.TaskID),
				)
				partFile, err := os.Open(partFileAbsPath)
				if err != nil {
					log.Fatalf("worker %d cannot open part file %v: %v", os.Getegid(), partFileAbsPath, err)
				}

				for {
					curKV := KeyValue{}
					cnt, err := fmt.Fscanf(partFile, "%v %v\n", &curKV.Key, &curKV.Value)

					if errors.Is(err, io.EOF) {
						break
					}

					if err != nil {
						log.Fatalf("worker %d read part file %v error: %v", os.Getpid(), partFileAbsPath, err)
					}

					if cnt != 2 {
						log.Fatalf("part file %v format unmatched, scanned %v.", partFileAbsPath, cnt)
					}

					allKV = append(allKV, curKV)
				}

				partFile.Close()
			}

			sort.Sort(ByKey(allKV))

			beg, end := 0, 0
			for beg < len(allKV) {

				curKey := allKV[beg].Key
				for end < len(allKV) && curKey == allKV[end].Key {
					end++
				}

				values := make([]string, 0)
				for i := beg; i < end; i++ {
					values = append(values, allKV[i].Value)
				}

				reduceRes := reducef(curKey, values)
				fmt.Fprintf(outputFile, "%v %v\n", curKey, reduceRes)

				beg = end

			}
			outputFile.Close()

			workspaceDirAbsPath, _ := filepath.Abs(workspaceDirRelaPath)
			if ok := call("Coordinator.ReportTaskDone", &ReportTaskDoneArgs{
				TaskType:          ReduceTaskType,
				TaskID:            task.TaskID,
				DispatchTimestamp: task.DispatchTimestamp,
				WorkspaceAbsPath:  workspaceDirAbsPath,
			}, &ReportTaskDoneReply{}); ok == false {
				log.Fatalf("worker %d cannot report task done.", os.Getpid())
			}

		case ExitType:
			log.Printf("worker %d exited.", os.Getpid())
			return
		}

	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil {
		log.Printf("worker %d call %v failed: %v", os.Getpid(), rpcname, err)
		return false
	}

	return true
}
