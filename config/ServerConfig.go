package config

type ServerConfig struct {
	RaftGroups           []RaftGroup       `json:"raftGroups"`
	SmartContractRaftMap map[string]string `json:"smartContractRaftMap"`
}

type RaftGroup struct {
	RaftID    string    `json:"raftID"`
	Addresses []Address `json:"addresses"`
}

type Address struct {
	Host string `json:"host"`
	Port string `json:"port"`
}
