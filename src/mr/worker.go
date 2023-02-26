package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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
	for true {
		assignArgs := &AssignArgs{}
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
			time.Sleep(time.Second)
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
	var err error
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
	err = file.Close()
	if err != nil {
		fmt.Println(err)
		log.Fatalf("can not close file %v", filename)
	}
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
			log.Fatal(err)
		}
	}
	for i, f := range files {
		oname := fmt.Sprintf("mr-%v-%v", assignReply.TaskId, i) // mr-X-Y
		os.Rename(fmt.Sprintf("./%v", f.Name()), fmt.Sprintf("./%v", oname))
		f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	completeArgs := CompleteArgs{TaskId: assignReply.TaskId, TaskType: assignReply.TaskType}
	completeReply := CompleteReply{}
	ok := false
	for !ok {
		ok = call("Coordinator.Complete", &completeArgs, &completeReply)
		time.Sleep(time.Second)
	}
	return nil
}

func handleReduceTask(assignArgs *AssignArgs, assignReply *AssignReply, reducef func(string, []string) string) error {
	kva := []KeyValue{}
	for _, filename := range assignReply.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			continue
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})
	ofile, err := os.CreateTemp(".", "*")
	if err != nil {
		log.Fatal(err)
	}
	i := 0
	for i < len(kva) {
		j := i
		values := []string{}
		for j < len(kva) && kva[j].Key == kva[i].Key {
			values = append(values, kva[j].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j + 1
	}
	oname := fmt.Sprintf("mr-%v", assignReply.TaskId)
	os.Rename(fmt.Sprintf("./%v", ofile.Name()), fmt.Sprintf("./%v", oname))
	ok := false
	for !ok {
		completeArgs := CompleteArgs{TaskId: assignReply.TaskId, TaskType: assignReply.TaskType}
		completeReply := CompleteReply{}
		ok = call("Coordinator.Complete", &completeArgs, &completeReply)
		time.Sleep(time.Second)
	}
	return nil
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
