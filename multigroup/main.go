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
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/lni/dragonboat/v4"
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
	// initial nodes count is three, their addresses are also fixed
	// this is for simplicity
	addresses = []string{
		"localhost:63001",
		"localhost:63002",
		"localhost:63003",
	}
)

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

func main() {

	replicaID := flag.Int("replicaid", 1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	join := flag.Bool("join", false, "Joining a new node")
	flag.Parse()

	if len(*addr) == 0 && *replicaID > 3 || *replicaID < 1 {
		fmt.Fprintf(os.Stderr, "invalid nodeid %d, it must be 1, 2 or 3", *replicaID)
		os.Exit(1)
	}

	// https://github.com/golang/go/issues/17393
	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}

	initialMembers := make(map[uint64]string)
	if !*join {
		for idx, v := range addresses {
			// key is the ReplicaID, ReplicaID is not allowed to be 0
			// value is the raft address
			initialMembers[uint64(idx+1)] = v
		}
	}

	var nodeAddr string
	if len(*addr) != 0 {
		nodeAddr = *addr
	} else {
		nodeAddr = initialMembers[uint64(*replicaID)]
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
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	datadir := filepath.Join(
		"example-data",
		//"multigroup-data",
		fmt.Sprintf("node%d", *replicaID))

	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 400,
		RaftAddress:    nodeAddr,
		//ListenAddress: "0.0.0.0:8094",
		// RaftRPCFactory: rpc.NewRaftGRPC,
	}
	// create a NodeHost instance. it is a facade interface allowing access to
	// all functionalities provided by dragonboat.
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}
	defer nh.Close()

	// start the first cluster
	// we use ExampleStateMachine as the IStateMachine for this cluster, its
	// behaviour is identical to the one used in the Hello World example.
	rc.ShardID = shardID1
	if err := nh.StartOnDiskReplica(initialMembers, *join, NewDiskKV, rc); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
		os.Exit(1)
	}

	// start the second cluster
	// we use SecondStateMachine as the IStateMachine for the second cluster
	rc.ShardID = shardID2
	if err := nh.StartOnDiskReplica(initialMembers, *join, NewDiskKV, rc); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
		os.Exit(1)
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

			// neVal := "put" + " " + randstr.Hex(8) + " " + randstr.Hex(16)
			// ch <- neVal
			ch <- s
		}
	})
	printUsage()

	raftStopper.RunWorker(func() {
		// use NO-OP client session here
		// check the example in godoc to see how to use a regular client session
		cs1 := nh.GetNoOPSession(shardID1)
		cs2 := nh.GetNoOPSession(shardID2)
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				// neVal := "put" + " " + randstr.Hex(8) + " " + randstr.Hex(16)

				// fmt.Println("================v", v)
				msg := strings.Replace(v, "\n", "", 1)

				key1 := randstr.Hex(8)
				val1 := randstr.Hex(16)
				rt, key, val, ok := parseCommand(msg)

				// if !ok {
				// 	fmt.Fprintf(os.Stderr, "invalid input\n")
				// 	printUsage()
				// 	continue
				// }

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if rt == GET {
					// if strings.HasSuffix(msg, "?") {
					// user message ends with "?", make a proposal to update the second
					// raft group
					result, err := nh.SyncRead(ctx, shardID2, []byte(key))
					if err != nil {
						fmt.Fprintf(os.Stderr, "SyncRead returned error %v\n", err)
					} else {
						fmt.Fprintf(os.Stdout, "query key: %s, result: %s\n", key, result)
					}
					// } else {
					// 	// message not ends with "?", make a proposal to update the first
					// 	// raft group
					// result1, err1 := nh.SyncRead(ctx, shardID1, []byte(key))
					// if err1 != nil {
					// 	fmt.Fprintf(os.Stderr, "SyncRead returned error %v\n", err1)
					// } else {
					// 	fmt.Fprintf(os.Stdout, "query key: %s, result: %s\n", key, result1)
					// }
					// }
				} else {

					for i := 0; i < 1000; i++ {
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
						// if strings.HasSuffix(msg, "?") {
						// user message ends with "?", make a proposal to update the second
						// raft group
						_, err = nh.SyncPropose(ctx, cs2, data)
						// } else {
						// 	// message not ends with "?", make a proposal to update the first
						// 	// raft group
						_, err = nh.SyncPropose(ctx, cs1, data)
						// }
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
