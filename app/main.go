package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	cf "multiraft/config"
	db "multiraft/database"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/goutils/syncutil"
	"github.com/thanhpk/randstr"
)

type RequestType uint64

const (
	PUT RequestType = iota
	GET
)

var (
	errNotMembershipChange = errors.New("not a membership change request")
)

func main() {

	replicaID := flag.Int("replicaid", 1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	join := flag.Bool("join", false, "Joining a new node")
	raftGroup := flag.Int("raftGroup", 100, "Number of raftGroup")
	flag.Parse()

	configdir := filepath.Join(
		"config",
		"config.json")

	serverConfig := LoadConfiguration(configdir)

	fmt.Println(serverConfig.RaftGroups[0].Addresses[0])

	shardID1 := serverConfig.RaftGroups[0].RaftID
	shardID2 := serverConfig.RaftGroups[1].RaftID

	if len(*addr) == 0 && *replicaID > 3 || *replicaID < 1 {
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
		for idx, v := range serverConfig.RaftGroups[0].Addresses {
			initialMembers[uint64(idx+1)] = v
		}

	}

	if !*join {
		for idx, v := range serverConfig.RaftGroups[1].Addresses {
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

	fmt.Println("nodeAddr", nodeAddr)
	fmt.Println("nodeAddr1", nodeAddr1)
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

	if *raftGroup == 1 {
		rc.ShardID = shardID1
		if err := nh.StartOnDiskReplica(initialMembers, *join, db.NewDiskKV, rc); err != nil {
			fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
			os.Exit(1)
		}
	} else {
		rc1.ShardID = shardID2
		if err := nh1.StartOnDiskReplica(initialMembers1, *join, db.NewDiskKV, rc1); err != nil {
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
				nh.Close()
				return
			}

			ch <- s
		}
	})
	printUsage()

	raftStopper.RunWorker(func() {

		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				msg := strings.Replace(v, "\n", "", 1)

				rt, key, val, _ := parseCommand(msg)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
				if cmd, addr, replicaID, err := splitMembershipChangeCmd(msg); err == nil {
					// input is a membership change request
					makeMembershipChange(nh, nh1, cmd, addr, replicaID, *raftGroup, shardID1, shardID2)
				} else {
					if rt == GET {
						if *raftGroup == 1 {
							fmt.Println("Entered in get call of raft group 1 ***********************")
							result, err := nh.SyncRead(ctx, shardID1, []byte(key))
							if err != nil {
								fmt.Fprintf(os.Stderr, "SyncRead returned error %v\n", err)
							} else {
								fmt.Fprintf(os.Stdout, "query key: %s, result: %s\n", key, result)
							}
						} else if *raftGroup == 2 {
							fmt.Println("Entered in get call of raft group 2 ***********************")
							result, err := nh1.SyncRead(ctx, shardID2, []byte(key))
							if err != nil {
								fmt.Fprintf(os.Stderr, "SyncRead returned error %v\n", err)
							} else {
								fmt.Fprintf(os.Stdout, "query key: %s, result: %s\n", key, result)
							}
						}

					} else {

						for i := 0; i < 10; i++ {
							key1 := randstr.Hex(8)
							val1 := randstr.Hex(16)

							fmt.Println("================key", key1)
							fmt.Println("================val", val1)
							fmt.Println(val)

							kv := &db.KVData{
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

func LoadConfiguration(file string) cf.ServerConfig {
	var config cf.ServerConfig
	configFile, err := os.Open(file)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)
	return config

}

func makeMembershipChange(nh *dragonboat.NodeHost, nh1 *dragonboat.NodeHost,
	cmd string, addr string, replicaID uint64, raftGroup int, shardID1 uint64, shardID2 uint64) {
	var rs *dragonboat.RequestState
	var err error
	if cmd == "add" {
		// orderID is ignored in standalone mode
		if raftGroup == 1 {
			rs, err = nh.RequestAddReplica(shardID1, replicaID, addr, 0, 3*time.Second)
		} else {
			rs, err = nh1.RequestAddReplica(shardID2, replicaID, addr, 0, 3*time.Second)
		}
	} else if cmd == "remove" {
		if raftGroup == 1 {
			rs, err = nh.RequestDeleteReplica(shardID1, replicaID, 0, 3*time.Second)
		} else {
			rs, err = nh1.RequestDeleteReplica(shardID2, replicaID, 0, 3*time.Second)
		}
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
