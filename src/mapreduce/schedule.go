package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	//a rotune to listen completed task
	finishedTaskCnt := 0
	finsishTask := make(chan bool)
	allTasksFinish := make(chan bool)
	go func() {
		for {
			if finishedTaskCnt == ntasks {
				break
			}
			<-finsishTask
			finishedTaskCnt++
		}
		allTasksFinish <- true
	}()

	//a rotune to listen failed task and reschedule
	doTask := make(chan int)
	go func(){
		for j:=0 ;j<ntasks ;j++{
			doTask <- j
		}
	}()
	for {
		select {
		case <-allTasksFinish:
			fmt.Println("All tasks finished!")
			fmt.Printf("Schedule: %v done\n", phase)
			return
		case i := <-doTask:
			go func(tn int, freeWorker chan string) {
				worker := <-freeWorker
				//fmt.Printf("schedule phase:%s, taskNumber:%d/%d, mapfiles:%d\n", phase, tn, ntasks, len(mapFiles))
				args := DoTaskArgs{
					JobName:       jobName,
					Phase:         phase,
					TaskNumber:    tn,
					NumOtherPhase: n_other,
				}
				if phase == mapPhase {
					args.File = mapFiles[tn]
				}
				var reply DoTaskReply

				if call(worker, "Worker.DoTask", &args, &reply){
					finsishTask <- true
				}else{
					doTask <- tn
				}
				freeWorker <- worker
			}(i, registerChan)
		}
	}
}
