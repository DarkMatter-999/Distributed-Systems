package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"

	//"io"
	"log"
	"net/rpc"
	"time"
)

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type KeyValue struct {
	Key   string
	Value string
}

type MapF func(string, string) []KeyValue
type ReduceF func(string, []string) string

type HostAddress struct {
	Host string
	Port string
}

func Worker(host HostAddress, mapf MapF, reducef ReduceF) {
	//log.SetOutput(io.Discard)

	for {
		time.Sleep(time.Second)
		task := Task{}
		if HeartBeat(host) {
			call(host, "MasterCoordinator.RequestTask", &Empty{}, &task)

			if task.Operation == ToWait {
				continue
			}

			if task.IsMap {
				log.Printf("received map job %s", task.Mapfunc.Filename)
				err := handleMap(host, task, mapf)
				if err != nil {
					log.Fatalf(err.Error())
					return
				}
			} else {
				log.Printf("received reduce job %d %v", task.Reducefunc.Id, task.Reducefunc.IntermediateFilenames)
				err := handleReduce(host, task, reducef)
				if err != nil {
					log.Fatalf(err.Error())
					return
				}
			}
		} else {
			time.Sleep(5 * time.Second)
		}
	}
}

func handleMap(host HostAddress, task Task, mapf MapF) error {
	filename := task.Mapfunc.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()

	kva := mapf(filename, string(content))
	var encoders []*json.Encoder
	for i := 0; i < task.NReduce; i++ {
		f, err := os.Create(fmt.Sprintf("mr-tmp-%d-%d", task.Mapfunc.Id, i))
		if err != nil {
			log.Fatalf("cannot create intermediate result file")
		}
		encoders = append(encoders, json.NewEncoder(f))
	}

	for _, kv := range kva {
		_ = encoders[ihash(kv.Key)%task.NReduce].Encode(&kv)
	}

	call(host, "MasterCoordinator.Finish", &FinishArgs{MapDone: true, Id: task.Mapfunc.Id}, &Empty{})
	return nil
}

func handleReduce(host HostAddress, task Task, reducef ReduceF) error {
	var kva []KeyValue
	for _, filename := range task.Reducefunc.IntermediateFilenames {
		iFile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(iFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		err = iFile.Close()
		if err != nil {
			log.Fatalf("cannot close %v", filename)
		}
	}

	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", task.Reducefunc.Id)
	temp, err := os.CreateTemp(".", oname)
	if err != nil {
		log.Fatalf("cannot create reduce result tempfile %s", oname)
		return err
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		_, _ = fmt.Fprintf(temp, "%v %v\n", kva[i].Key, output)

		i = j
	}
	err = os.Rename(temp.Name(), oname)
	if err != nil {
		return err
	}

	call(host, "MasterCoordinator.Finish", &FinishArgs{MapDone: false, Id: task.Reducefunc.Id}, &Empty{})
	return nil
}

func HeartBeat(host HostAddress) bool {
	if call(host, "MasterCoordinator.HeartBeat", &Empty{}, &Empty{}) {
		return true
	}
	return false
}

func call(master HostAddress, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", master.Host+":"+master.Port)
	if err != nil {
		log.Println("dialing:", rpcname, " ", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
