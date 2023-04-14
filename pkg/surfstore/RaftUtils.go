package surfstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}
	metaStoreMutex := sync.RWMutex{}
	logMutex := sync.RWMutex{}

	group_id_addr_maps := make(map[int]string, 0)
	for id1, addr := range config.RaftAddrs {
		group_id_addr_maps[id1] = addr
	}

	server := RaftSurfstore{
		isLeader:      false,
		isLeaderMutex: &isLeaderMutex,
		term:          int64(0),
		metaStore:     NewMetaStore(config.BlockAddrs),
		log:           make([]*UpdateOperation, 0),
		isCrashed:     false,

		isCrashedMutex: &isCrashedMutex,
		notCrashedCond: sync.NewCond(&isCrashedMutex),
		metaStoreMutex: &metaStoreMutex,
		logMutex:       &logMutex,

		group_id_addr_maps:       group_id_addr_maps,
		self_server_ID:           id,
		commitIndex:              -1,
		nextIndex_for_follower:   make(map[int]int, 0),
		matchIndex_for_followers: make(map[int]int, 0),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	// panic("todo")
	local_meta_addr := server.group_id_addr_maps[int(server.self_server_ID)]
	grpc_server := grpc.NewServer()
	RegisterMetaStoreServer(grpc_server, server.metaStore)
	RegisterRaftSurfstoreServer(grpc_server, server)
	listener, err := net.Listen("tcp", local_meta_addr)
	if err != nil {
		return err
	}

	// log.Printf("Begin Serving gRPC Server")
	if err := grpc_server.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %s", err)
	}
	return nil
}
