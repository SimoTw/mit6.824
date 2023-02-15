package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	done := false
	for !done {
		assignArgs := &AssignArgs{WorkerId: -1}
		assignReply := &AssignReply{}
		ok := call("Coordinator.Assign", &assignArgs, &assignReply)
		if ok {
			switch assignReply.TaskType {
			case MAP_TASK:
				err := handleMapTask(assignArgs, assignReply, mapf)
				if err != nil {
					fmt.Println(err)
				}
				break
			case REDUCE_TASK:
				err := handleReduceTask(assignArgs, assignReply, reducef)
				if err != nil {
					fmt.Println(err)
				}
				break
			default:
				break
			}

		} else {
			fmt.Println("call faild")
			time.Sleep(time.Second)
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func handleMapTask(assignArgs *AssignArgs, assignReply *AssignReply, mapf func(string, string) []KeyValue) error {
	filename := assignReply.Filename
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		log.Fatalf("can not read file %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Println(err)
		log.Fatalf("can not read file %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	files := []*os.File{}
	for i := 0; i < assignReply.NReduce; i++ {
		f, err := os.CreateTemp(".", "*")
		if err != nil {
			log.Fatal(err)
		}
		files = append(files, f)
	}
	encs := []*json.Encoder{}
	for _, f := range files {
		enc := json.NewEncoder(f)
		encs = append(encs, enc)
	}
	for _, kv := range kva {
		i := ihash(kv.Key) % assignReply.NReduce
		enc := encs[i]
		err := enc.Encode(&kv)
		if err != nil {
			f := files[i]
			f.Close() // ignore error; Write error takes precedence
			log.Fatal(err)
		}
	}
	for i, f := range files {
		oname := fmt.Sprintf("mr-%v-%v", assignReply.WorkerId, i) // mr-X-Y
		os.Rename(fmt.Sprintf("./%v", f.Name()), fmt.Sprintf("./%v", oname))
		err = f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	completeArgs := CompleteArgs{Filename: assignReply.Filename, TaskType: assignReply.TaskType}
	completeReply := CompleteReply{}
	ok := false
	for !ok {
		ok = call("Coordinator.Complete", &completeArgs, &completeReply)
		time.Sleep(time.Second)
	}
	return nil
}

func handleReduceTask(assignArgs *AssignArgs, assignReply *AssignReply, reducef func(string, []string) string) error {
	return nil
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
