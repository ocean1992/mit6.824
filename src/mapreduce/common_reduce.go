package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

type InterData []KeyValue

func (data InterData) Len() int           { return len(data) }
func (data InterData) Less(i, j int) bool { return data[i].Key < data[j].Key }
func (data InterData) Swap(i, j int)      { data[i], data[j] = data[j], data[i] }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var interData InterData
	for m := 0; m < nMap; m++ {
		interFile := reduceName(jobName, m, reduceTask)
		fmt.Printf("The reduce read interFile %s\n", interFile)
		reader, _ := os.Open(interFile)
		dec := json.NewDecoder(reader)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			interData = append(interData, kv)
		}
	}
	sort.Sort(interData)
	writer, _ := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE, 0755)
	enc := json.NewEncoder(writer)
	var values []string
	for i := 0; i < interData.Len(); i++ {
		if i == 0 {
			values = []string{interData[i].Value}
		} else {
			if interData[i].Key != interData[i-1].Key {
				enc.Encode(KeyValue{interData[i-1].Key, reduceF(interData[i-1].Key, values)})
				values = []string{interData[i].Value}
			} else {
				values = append(values, interData[i].Value)
			}
		}
	}
	enc.Encode(KeyValue{interData[interData.Len()-1].Key, reduceF(interData[interData.Len()-1].Key, values)})
	writer.Close()
}
