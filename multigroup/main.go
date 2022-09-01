/*multigroup is an example program for dragonboat demonstrating how multiple
raft groups can be used in an user application.
*/
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/goutils/syncutil"
	"github.com/thanhpk/randstr"
)

type RequestType uint64

const (
	// we use two raft groups in this example, they are identified by the cluster
	// ID values below
	shardID1 uint64 = 100
	shardID2 uint64 = 101
)

const (
	PUT RequestType = iota
	GET
)

var (
	addresses = []string{
		"localhost:63001",
		"localhost:63002",
		"localhost:63003",
	}
)

var (
	addresses2 = []string{
		"localhost:63004",
		"localhost:63005",
		"localhost:63006",
	}
)

func main() {

	replicaID := flag.Int("replicaid", 1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	join := flag.Bool("join", false, "Joining a new node")
	records := flag.Int("records", 100, "Number of records to be inserted")
	raftGroup := flag.Int("raftGroup", 1, "Raft group the node belongs too")

	flag.Parse()

	fmt.Println("number of records", *records)

	if len(*addr) == 0 && *replicaID > 6 || *replicaID < 1 {
		fmt.Fprintf(os.Stderr, "invalid nodeid %d, it must be 1, 2 or 3", *replicaID)
		os.Exit(1)
	}

	// https://github.com/golang/go/issues/17393
	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}

	initialMembers := make(map[uint64]string)
	initialMembers2 := make(map[uint64]string)
	if !*join {
		for idx, v := range addresses {
			// key is the ReplicaID, ReplicaID is not allowed to be 0
			// value is the raft address
			initialMembers[uint64(idx+1)] = v
		}
	}

	if !*join {
		for idx, v := range addresses2 {
			// key is the ReplicaID, ReplicaID is not allowed to be 0
			// value is the raft address
			initialMembers2[uint64(idx+1)] = v
		}
	}

	var nodeAddr string
	if len(*addr) != 0 {
		nodeAddr = *addr
	} else if *replicaID <= 3 {
		nodeAddr = initialMembers[uint64(*replicaID)]
	} else {
		nodeAddr = initialMembers2[uint64(*replicaID-3)]
	}
	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)
	// change the log verbosity
	logger.GetLogger("raft").SetLevel(logger.DEBUG)
	logger.GetLogger("rsm").SetLevel(logger.DEBUG)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
	// config for raft
	// note the ShardID value is not specified here
	rc := config.Config{
		ReplicaID:          uint64(*replicaID),
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    100,
		CompactionOverhead: 5,
	}
	datadir := filepath.Join(
		"shard1-data",
		//"multigroup-data",
		fmt.Sprintf("node%d", *replicaID))

	datadir2 := filepath.Join(
		"shard2-data",
		//"multigroup-data",
		fmt.Sprintf("node%d", *replicaID))

	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddr,
		//ListenAddress:  "0.0.0.0:8094",
		// RaftRPCFactory: rpc.NewRaftGRPC,
	}

	nhc2 := config.NodeHostConfig{
		WALDir:         datadir2,
		NodeHostDir:    datadir2,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddr,
		//ListenAddress:  "0.0.0.0:8094",
		// RaftRPCFactory: rpc.NewRaftGRPC,
	}

	var nh *dragonboat.NodeHost
	var nh2 *dragonboat.NodeHost
	var err error
	// create a NodeHost instance. it is a facade interface allowing access to
	// all functionalities provided by dragonboat.
	if *raftGroup == 1 {
		nh, err = dragonboat.NewNodeHost(nhc)
	} else {
		nh2, err = dragonboat.NewNodeHost(nhc2)
	}
	if err != nil {
		panic(err)
	}
	defer nh.Close()

	rc.ShardID = shardID1
	if *raftGroup == 1 {
		rc.ShardID = shardID1
		if err := nh.StartOnDiskReplica(initialMembers, *join, NewDiskKV, rc); err != nil {
			fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
			os.Exit(1)
		}
	}

	if *raftGroup == 2 {
		rc.ShardID = shardID2
		if err := nh2.StartOnDiskReplica(initialMembers2, *join, NewDiskKV, rc); err != nil {
			fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
			os.Exit(1)
		}
	}

	raftStopper := syncutil.NewStopper()
	consoleStopper := syncutil.NewStopper()
	ch := make(chan string, 16)

	consoleStopper.RunWorker(func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				close(ch)
				return
			}
			if s == "exit\n" {
				raftStopper.Stop()
				// no data will be lost/corrupted if nodehost.Stop() is not called
				nh.Close()
				return
			}

			ch <- s
		}
	})
	printUsage()

	raftStopper.RunWorker(func() {
		// use NO-OP client session here
		// check the example in godoc to see how to use a regular client session
		var cs1 *client.Session
		var cs2 *client.Session
		if *raftGroup == 1 {
			cs1 = nh.GetNoOPSession(shardID1)
		}
		if *raftGroup == 2 {
			cs2 = nh2.GetNoOPSession(shardID1)
		}
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				msg := strings.Replace(v, "\n", "", 1)

				key1 := randstr.Hex(8)
				val1 := randstr.Hex(16)
				rt, key, val, ok := parseCommand(msg)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if rt == GET && *replicaID <= 3 {
					result, err := nh.SyncRead(ctx, shardID1, []byte(key))
					if err != nil {
						fmt.Fprintf(os.Stderr, "SyncRead returned error %v\n", err)
					} else {
						fmt.Fprintf(os.Stdout, "query key: %s, result: %s\n", key, result)
					}
				} else if rt == GET && *replicaID > 3 {
					result, err := nh.SyncRead(ctx, shardID2, []byte(key))
					if err != nil {
						fmt.Fprintf(os.Stderr, "SyncRead returned error %v\n", err)
					} else {
						fmt.Fprintf(os.Stdout, "query key: %s, result: %s\n", key, result)
					}
				} else {

					for i := 0; i < *records; i++ {
						key1 = randstr.Hex(8)
						val1 = randstr.Hex(16)

						fmt.Println("================key", key1)
						fmt.Println("================val", val1)
						fmt.Println(val)

						kv := &KVData{
							Key: key1,
							Val: val1,
						}
						data, _err := json.Marshal(kv)
						if _err != nil {
							panic(err)
						}

						//if hash(key)%2 == 0 {
						//	fmt.Println("first machine")
						_, err = nh.SyncPropose(ctx, cs2, data)
						//} else {
						//fmt.Println("second machine")
						_, err = nh.SyncPropose(ctx, cs1, data)
						//}
						if err != nil {
							fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
						}
					}
				}

				cancel()
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})
	raftStopper.Wait()
}

func parseCommand(msg string) (RequestType, string, string, bool) {
	parts := strings.Split(strings.TrimSpace(msg), " ")
	if len(parts) == 0 || (parts[0] != "put" && parts[0] != "get") {
		return PUT, "", "", false
	}
	if parts[0] == "put" {
		if len(parts) != 3 {
			return PUT, "", "", false
		}
		return PUT, parts[1], parts[2], true
	}
	if len(parts) != 2 {
		return GET, "", "", false
	}
	return GET, parts[1], "", true
}

func printUsage() {
	fmt.Fprintf(os.Stdout, "Usage - \n")
	fmt.Fprintf(os.Stdout, "put key1 value1\n")
	fmt.Fprintf(os.Stdout, "get key1\n")
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
