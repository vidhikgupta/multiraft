/*multigroup is an example program for dragonboat demonstrating how multiple
raft groups can be used in an user application.
*/
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
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
	errNotMembershipChange = errors.New("not a membership change request")
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

// makeMembershipChange makes membership change request.
func makeMembershipChange(nh *dragonboat.NodeHost,
	cmd string, addr string, replicaID uint64) {
	var rs *dragonboat.RequestState
	var err error
	if cmd == "add" {
		// orderID is ignored in standalone mode
		rs, err = nh.RequestAddReplica(shardID1, replicaID, addr, 0, 3*time.Second)
		rs, err = nh.RequestAddReplica(shardID2, replicaID, addr, 0, 3*time.Second)
	} else if cmd == "remove" {
		rs, err = nh.RequestDeleteReplica(shardID1, replicaID, 0, 3*time.Second)
		rs, err = nh.RequestDeleteReplica(shardID2, replicaID, 0, 3*time.Second)
	} else {
		panic("unknown cmd")
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "membership change failed, %v\n", err)
		return
	}
	select {
	case r := <-rs.CompletedC:
		if r.Completed() {
			fmt.Fprintf(os.Stdout, "membership change completed successfully\n")
		} else {
			fmt.Fprintf(os.Stderr, "membership change failed\n")
		}
	}
}

// splitMembershipChangeCmd tries to parse the input string as membership change
// request. ADD node request has the following expected format -
// add localhost:63100 4
// REMOVE node request has the following expected format -
// remove 4
func splitMembershipChangeCmd(v string) (string, string, uint64, error) {
	parts := strings.Split(v, " ")
	if len(parts) == 2 || len(parts) == 3 {
		cmd := strings.ToLower(strings.TrimSpace(parts[0]))
		if cmd != "add" && cmd != "remove" {
			return "", "", 0, errNotMembershipChange
		}
		addr := ""
		var replicaIDStr string
		var replicaID uint64
		var err error
		if cmd == "add" {
			addr = strings.TrimSpace(parts[1])
			replicaIDStr = strings.TrimSpace(parts[2])
		} else {
			replicaIDStr = strings.TrimSpace(parts[1])
		}
		if replicaID, err = strconv.ParseUint(replicaIDStr, 10, 64); err != nil {
			return "", "", 0, errNotMembershipChange
		}
		return cmd, addr, replicaID, nil
	}
	return "", "", 0, errNotMembershipChange
}

func main() {

	replicaID := flag.Int("replicaid", 1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	join := flag.Bool("join", false, "Joining a new node")
	records := flag.Int("records", 100, "Number of records to be inserted")
	flag.Parse()
	fmt.Println("number of records", *records)

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
		SnapshotEntries:    100,
		CompactionOverhead: 5,
	}
	datadir := filepath.Join(
		"example-data",
		"multigroup-data",
		fmt.Sprintf("node%d", *replicaID))

	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddr,
		//ListenAddress:  "0.0.0.0:8094",

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
				msg := strings.Replace(v, "\n", "", 1)

				key1 := randstr.Hex(8)
				val1 := randstr.Hex(16)
				rt, key, val, ok := parseCommand(msg)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if cmd, addr, replicaID, err := splitMembershipChangeCmd(msg); err == nil {
					// input is a membership change request
					makeMembershipChange(nh, cmd, addr, replicaID)
				} else {
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
				}

				cancel()
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})
	raftStopper.Wait()
}
