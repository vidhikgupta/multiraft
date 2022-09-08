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
		//"34.131.129.59:8094",
		//"34.93.228.166:8094",
		//	"34.100.168.202:8094",
	}
)

var (
	addresses1 = []string{
		"localhost:63004",
		"localhost:63005",
		"localhost:63006",
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

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func main() {

	replicaID := flag.Int("replicaid", 1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	join := flag.Bool("join", false, "Joining a new node")
	records := flag.Int("records", 100, "Number of records to be inserted")
	raftGroup := flag.Int("raftGroup", 100, "Number of raftGroup")
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
	initialMembers1 := make(map[uint64]string)
	if !*join {
		for idx, v := range addresses {
			// key is the ReplicaID, ReplicaID is not allowed to be 0
			// value is the raft address
			initialMembers[uint64(idx+1)] = v
		}

	}

	if !*join {
		for idx, v := range addresses1 {
			initialMembers1[uint64(idx+1)] = v
		}
	}

	var nodeAddr string
	var nodeAddr1 string
	if len(*addr) != 0 {
		nodeAddr = *addr
	} else if *raftGroup == 1 {
		nodeAddr = initialMembers[uint64(*replicaID)]
	} else {
		nodeAddr1 = initialMembers1[uint64(*replicaID)]
	}
	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)
	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr1)
	// change the log verbosity
	logger.GetLogger("raft").SetLevel(logger.DEBUG)
	logger.GetLogger("rsm").SetLevel(logger.DEBUG)
	logger.GetLogger("transport").SetLevel(logger.DEBUG)
	logger.GetLogger("grpc").SetLevel(logger.DEBUG)
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

	rc1 := config.Config{
		ReplicaID:          uint64(*replicaID),
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    100,
		CompactionOverhead: 5,
	}
	datadir := filepath.Join(
		"shard-data1",
		fmt.Sprintf("node%d", *replicaID))

	datadir1 := filepath.Join(
		"shard-data2",
		fmt.Sprintf("node%d", *replicaID))

	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddr,
		//ListenAddress:  "0.0.0.0:8094",
	}

	nhc1 := config.NodeHostConfig{
		WALDir:         datadir1,
		NodeHostDir:    datadir1,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddr1,
		//ListenAddress:  "0.0.0.0:8094",
	}
	// create a NodeHost instance. it is a facade interface allowing access to
	// all functionalities provided by dragonboat.

	var nh *dragonboat.NodeHost
	var nh1 *dragonboat.NodeHost
	var err error
	if *raftGroup == 1 {
		nh, err = dragonboat.NewNodeHost(nhc)
	} else {
		nh1, err = dragonboat.NewNodeHost(nhc1)
	}

	if err != nil {
		panic(err)

	}
	defer nh.Close()

	// start the first cluster
	// we use ExampleStateMachine as the IStateMachine for this cluster, its
	// behaviour is identical to the one used in the Hello World example.
	if *raftGroup == 1 {
		rc.ShardID = shardID1
		if err := nh.StartOnDiskReplica(initialMembers, *join, NewDiskKV, rc); err != nil {
			fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
			os.Exit(1)
		}
	} else {
		// start the second cluster
		// we use SecondStateMachine as the IStateMachine for the second cluster
		rc1.ShardID = shardID2
		if err := nh1.StartOnDiskReplica(initialMembers1, *join, NewDiskKV, rc1); err != nil {
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
				ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
				if rt == GET {
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
							fmt.Println("", err)
							panic(err)
						}

						if *raftGroup == 1 {
							cs1 := nh.GetNoOPSession(shardID1)
							_, err = nh.SyncPropose(ctx, cs1, data)

							if err != nil {
								fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
							}
						} else {
							cs2 := nh1.GetNoOPSession(shardID2)
							_, err = nh1.SyncPropose(ctx1, cs2, data)

							if err != nil {
								fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
							}
						}
					}
				}

				cancel()
				cancel1()
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})
	raftStopper.Wait()
}
