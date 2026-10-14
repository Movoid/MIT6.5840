package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]Entry
}

type Entry struct {
	value       string
	nextVersion rpc.DataVersion
}

func MakeKVServer() *KVServer {
	return &KVServer{data: make(map[string]Entry)}
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kvEntry, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = kvEntry.value
	reply.Version = kvEntry.nextVersion
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kvEntry, ok := kv.data[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		kv.data[args.Key] = Entry{value: args.Value, nextVersion: 1}
		reply.Err = rpc.OK
		return
	}

	if kvEntry.nextVersion != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	kvEntry.value = args.Value
	kvEntry.nextVersion++
	kv.data[args.Key] = kvEntry
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.ShardGroupId, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
