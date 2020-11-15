package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	SCHEDULE_TASK_SUCCESS     = "success"
	SCHEDULE_TASK_NOAVAILABLE = "noavailable" //调度不成功，
	SCHEDULE_TASK_DONE        = "done"        //已经全部调度完毕
)

type Master struct {
	// Your definitions here.
	mutex sync.Mutex
	cond  *sync.Cond

	files []string

	//map和reduce计数
	mapCount    int
	reduceCount int
	//已经完成的map和reduce任务数
	sucMapTask    int
	sucReduceTask int

	//管理字段,通道用于通信/等待
	mapChan    chan int //map 任务下标的通道
	reduceChan chan int //reduce 任务下标的通道
	//用于保存正在运行的任务，key是map地址，value是启动时间
	runningMapTask    map[int]int64
	runningReduceTask map[int]int64
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) askForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	//判断该worker有没有上次执行的complete task，先处理
	m.finishTask(args.CompleteTask)

	for {
		var ret string
		task, ret := m.scheduleTask()
		switch ret {
		case SCHEDULE_TASK_SUCCESS:
			reply.Task = *task
			reply.Done = false
			return nil
		case SCHEDULE_TASK_NOAVAILABLE:
			//需要等待
			m.cond.Wait()
		case SCHEDULE_TASK_DONE:
			reply.Done = true
			return nil
		}
	}

}

func (m *Master) scheduleTask() (*Task, string) {
	//先分配 map task，所有map task 都完成后再分配reduce task
	now = time.Now().Unix()
	select {
	case mapID := <-m.mapChan:
		task := &Task{
			Phase: TASK_PHASE_MAP,
			MapTask: MapTask{
				FileName:     m.files[mapID],
				MapIndex:     mapID,
				ReduceNumber: m.nReduce,
			},
		}
		m.runningMapTask[mapID] = now
		return task, SCHEDULE_TASK_SUCCESS
	default:
		//表示任务已经分配完毕，但可能还有正在运行的任务
		if len(m.runningMapTask) > 0 {
			return nil, SCHEDULE_TASK_NOAVAILABLE
		}
	}

	select {
	case reduceID := <-m.reduceChan:
		task := &Task{
			Phase: TASK_PHASE_REDUCE,
			ReduceTask: ReduceTask{
				MapNumber:   m.mMap,
				ReduceIndex: reduceIndex,
			},
		}
		m.runningReduceTask[reduceID] = now
		return task, SCHEDULE_TASK_SUCCESS
	default:
		if len(m.runningReduceTask) > 0 {
			return nil, SCHEDULE_TASK_NOAVAILABLE
		}
	}

	return nil, SCHEDULE_TASK_DONE
}

//处理已经处理成功的任务
func (m *Master) finishTask(task Task) {
	switch task.Phase {
	case TASK_PHASE_MAP:
		//判断是否在running里面
		if _, ok := m.runningMapTask[task.MapTask.MapIndex]; !ok {
			//可能已经被其他worker处理了,因为有超时重试的机制
			return
		}
		delete(m.runningMapTask, task.MapTask.MapIndex)
		m.completedMapTask += 1
		m.cond.Broadcast() //in order to avoid askfortask wait forever (for reduce paralissm pass)
	case TASK_PHASE_REDUCE:
		if _, ok := m.runningReduceTask[task.ReduceTask.ReduceIndex]; !ok {
			return
		}
		delete(m.runningReduceTask, task.ReduceTask.ReduceIndex)
		m.completedReduceTask += 1
		m.cond.Broadcast() //in order to avoid askfortask wait forever (for reduce paralissm pass)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	ret = m.mapCount == m.sucMapTask && m.reduceCount == m.sucReduceTask
	return ret
}

//判断任务是否完成，若未完成，判断是否超时（10s)，超时重新分配任务
func (m *Master) taskChecker() {
	const TIMEOUT = 10

	for {
		if m.Done() {
			return
		}

		m.cond.L.Lock()
		ret := false
		now := time.Now().Unix()
		for mapID, startTime := range m.runningMapTask {
			//超时
			if startTime+TIMEOUT < now {
				delete(m.runningMapTask, mapID)
				m.mapChan <- mapID //重新分配
				ret = true
			}
		}
		for reduceID, startTime := range m.runningReduceTask {
			if startTime+TIMEOUT < now {
				delete(m.runningReduceTask, reduceID)
				m.reduceChan <- reduceID //重新分配
				ret = true
			}
		}

		//广播有新任务重新分配
		if ret {
			m.cond.Broadcast()
		}

		m.cond.L.Unlock()
		time.Sleep(time.Second)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files
	m.cond = sync.NewCond(&m.mutex)

	m.mapCount = len(files)
	m.reduceCount = nReduce
	m.mapChan = make(chan int, m.mapCount)
	m.reduceChan = make(chan int, m.reduceCount)
	m.runningMapTask = make(map[int]int64, 0)
	m.runningReduceTask = make(map[int]int64, 0)

	//分配任务
	for i := 0; i < m.mapCount; i++ {
		m.mapChan <- i
	}
	for i := 0; i < m.reduceCount; i++ {
		m.reduceChan <- i
	}

	log.Println("start master: ", m)
	go m.taskChecker()

	m.server()
	return &m
}
