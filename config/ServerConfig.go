package config

type ServerConfig struct {
	RaftGroups           []RaftGroup       `json:"raftGroups"`
	SmartContractRaftMap map[string]string `json:"smartContractRaftMap"`
}

type RaftGroup struct {
	RaftID    uint64   `json:"raftID"`
	Addresses []string `json:"addresses"`
}

type Address struct {
	Host string `json:"host"`
	Port string `json:"port"`
}
