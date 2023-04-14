package SurfTest

import (
	"Raft_DisFileSys/pkg/surfstore"
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftNewLeaderPushesUpdates2(t *testing.T) {
	t.Logf("leader1 gets a request while the majority of the cluster is down. leader1 crashes. the other nodes come back. leader2 is elected")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// a majority of servers crashed
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	defer worker1.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	// instead of calling SyncClient(), directly call UpdateFile() to append logs
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = test.Clients[0].UpdateFile(ctx, &surfstore.FileMetaData{
		Filename: "test.txt",
		Version:  1,
		BlockHashList: func() []string {
			blockHashList := make([]string, 0)
			blockHashList = append(blockHashList, "0")
			return blockHashList
		}(),
	})
	if err == nil {
		t.Fatalf("Sync should have failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// leader1 crashed and others restores
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	t.Logf("Leader1 crashed.")
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	// leader2 is elected
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	for serverId := 0; serverId < len(test.Clients); serverId++ {
		state, err := test.Clients[serverId].GetInternalState(test.Context, &emptypb.Empty{})
		if err != nil {
			t.Fatalf("could not get internal state: " + err.Error())
		}
		if len(state.Log) != 1 {
			t.Fatalf(fmt.Sprintf("incorrect fileInfoMap length in Server%d", serverId))
		}
	}
}

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	// state, _ := test.Clients[leaderIdx].GetInternalState(test.Context, &emptypb.Empty{})
	// log.Print(state.Term)

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
		// state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		// log.Print(state.Term)
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		// log.Print(state.Term)
		if state == nil {
			t.Fatalf("Could not get state")
		}
		// panic("debug here")
		if state.Term != int64(1) {
			// log.Print(state.Term)
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		log.Print(state.Term)
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}
