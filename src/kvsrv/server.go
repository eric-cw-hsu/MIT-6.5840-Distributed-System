package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OperationType int

const (
	OPERATION_GET OperationType = iota
	OPERATION_PUT
	OPERATION_APPEND
)

type KVServer struct {
	mu      sync.Mutex
	kvStore map[string]string

	clientLastRequest map[int64]int64
	responseCache     map[int64]string
}

func (kv *KVServer) initialize() {
	kv.kvStore = make(map[string]string)
	kv.responseCache = make(map[int64]string)
	kv.clientLastRequest = make(map[int64]int64)
}

func (kv *KVServer) removeStaleCache(clientId, requestId int64, op OperationType) {
	if lastRequestId, ok := kv.clientLastRequest[clientId]; ok {
		if lastRequestId != requestId {
			// remove cache
			delete(kv.responseCache, lastRequestId)
		}
	}

	if op != OPERATION_GET {
		kv.clientLastRequest[clientId] = requestId
	}
}

func (kv *KVServer) storeResponse(requestId int64, value string) {
	kv.responseCache[requestId] = value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.removeStaleCache(args.ClientId, args.RequestId, OPERATION_GET)

	value, ok := kv.kvStore[args.Key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.removeStaleCache(args.ClientId, args.RequestId, OPERATION_PUT)

	if _, ok := kv.responseCache[args.RequestId]; ok {
		reply.Value = kv.responseCache[args.RequestId]
		return
	}

	kv.kvStore[args.Key] = args.Value
	reply.Value = ""

	kv.storeResponse(args.RequestId, "")
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.removeStaleCache(args.ClientId, args.RequestId, OPERATION_APPEND)

	if _, ok := kv.responseCache[args.RequestId]; ok {
		reply.Value = kv.responseCache[args.RequestId]
		return
	}

	oriData := ""
	if _, ok := kv.kvStore[args.Key]; ok {
		oriData = kv.kvStore[args.Key]
	}

	kv.kvStore[args.Key] = oriData + args.Value
	reply.Value = oriData

	kv.storeResponse(args.RequestId, oriData)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.initialize()

	return kv
}
