package SurfTest

import (
	// "Raft_DisFileSys/pkg/surfstore"

	"Raft_DisFileSys/pkg/surfstore"
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func MajorityCrashRecover(test TestInfo) {
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	time.Sleep(1 * time.Second)
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
}

func MajorityRecover(test TestInfo, run *bool) {
	for !(*run) {
		log.Print("main thread blocked")
	}
}

func TestRaftRecoverable(t *testing.T) {
	t.Logf("leader1 gets a request while all other nodes are crashed. the crashed nodes recover. ")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	defer worker1.CleanUp()

	go MajorityCrashRecover(test)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := test.Clients[0].UpdateFile(ctx, &surfstore.FileMetaData{
		Filename: "test.txt",
		Version:  1,
		BlockHashList: func() []string {
			blockHashList := make([]string, 0)
			blockHashList = append(blockHashList, "0")
			return blockHashList
		}(),
	})
	t.Log(err)
	if err != nil {
		t.Fatalf("Sync should be completed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
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

// func TestRaftNewLeaderPushesUpdates(t *testing.T) {
// 	t.Logf("leader1 gets a request while the majority of the cluster is down. leader1 crashes. the other nodes come back. leader2 is elected")
// 	cfgPath := "./config_files/5nodes.txt"
// 	test := InitTest(cfgPath)
// 	defer EndTest(test)

// 	// a majority of servers crashed
// 	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
// 	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
// 	test.Clients[3].Crash(test.Context, &emptypb.Empty{})
// 	t.Logf("follower 1,2,3 crashed.")

// 	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
// 	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	worker1 := InitDirectoryWorker("test0", SRC_PATH)
// 	defer worker1.CleanUp()

// 	//clients add different files
// 	file1 := "multi_file1.txt"
// 	err := worker1.AddFile(file1)
// 	if err != nil {
// 		t.FailNow()
// 	}

// 	// instead of calling SyncClient(), directly call UpdateFile() to append logs
// 	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
// 	defer cancel()
// 	_, err = test.Clients[0].UpdateFile(ctx, &surfstore.FileMetaData{
// 		Filename: "test.txt",
// 		Version:  1,
// 		BlockHashList: func() []string {
// 			blockHashList := make([]string, 0)
// 			blockHashList = append(blockHashList, "0")
// 			return blockHashList
// 		}(),
// 	})
// 	if err == nil {
// 		t.Fatalf("Sync should have failed")
// 	}
// 	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	// leader1 crashed and others restores
// 	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
// 	t.Logf("Leader1 crashed.")
// 	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
// 	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
// 	test.Clients[3].Restore(test.Context, &emptypb.Empty{})
// 	t.Logf("follower 1,2,3 restored.")

// 	// leader2 is elected
// 	test.Clients[4].SetLeader(test.Context, &emptypb.Empty{})
// 	t.Logf("Leader2 is elected.")

// 	test.Clients[4].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	for _, server := range test.Clients {
// 		// server.SendHeartbeat(test.Context, &emptypb.Empty{})
// 		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
// 		log.Print(state)
// 	}

// 	for serverId := 0; serverId < len(test.Clients); serverId++ {
// 		state, err := test.Clients[serverId].GetInternalState(test.Context, &emptypb.Empty{})
// 		if err != nil {
// 			t.Fatalf("could not get internal state: " + err.Error())
// 		}
// 		// t.Log(serverId, state)
// 		if len(state.Log) != 1 {
// 			t.Fatalf(fmt.Sprintf("incorrect fileInfoMap length in Server%d", serverId))
// 		}
// 	}
// }

func TestRaftNewLeaderPushesUpdatesFiveNodes(t *testing.T) {
	t.Logf("leader1 gets a request while the majority of the cluster is down. leader1 crashes. the other nodes come back. leader2 is elected")
	cfgPath := "./config_files/5nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// a majority of servers crashed
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	test.Clients[3].Crash(test.Context, &emptypb.Empty{})
	test.Clients[4].Crash(test.Context, &emptypb.Empty{})

	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	defer worker1.CleanUp()

	// instead of calling SyncClient(), directly call UpdateFile() to append logs
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := test.Clients[0].UpdateFile(ctx, &surfstore.FileMetaData{
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

	// leader1 crashed and others restores
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	t.Logf("Leader1 crashed.")
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[3].Restore(test.Context, &emptypb.Empty{})
	test.Clients[4].Restore(test.Context, &emptypb.Empty{})

	// leader2 is elected
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// gt, err := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	// if err != nil {
	// 	t.Fatalf("could not get internal state: " + err.Error())
	// }
	for serverId := 0; serverId < len(test.Clients); serverId++ {
		state, err := test.Clients[serverId].GetInternalState(test.Context, &emptypb.Empty{})
		if err != nil {
			t.Fatalf("could not get internal state: " + err.Error())
		}
		t.Log(state)
		if len(state.GetLog()) != 1 {
			t.Fatalf(fmt.Sprintf("incorrect fileInfoMap length in Server%d", serverId))
		}
		if len(state.GetMetaMap().FileInfoMap) > 0 {
			t.Fatalf(fmt.Sprintf("incorrect fileInfoMap length in Server%d", serverId))
		}
		// t.Log(state.MetaMap)
	}
}

func TestRaftLogsConsistent(t *testing.T) {
	t.Logf("leader1 gets a request while a minority of the cluster is down. leader1 crashes. the other crashed nodes are restored. leader2 gets a request. leader1 is restored.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// a minority of servers crashed
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	// worker3 := InitDirectoryWorker("test2", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()
	// defer worker3.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file2.txt"
	// file3 := "multi_file3.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	// err = worker3.AddFile(file3)
	// if err != nil {
	// 	t.FailNow()
	// }

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// leader1 crashed and client2 restores
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	t.Logf("Leader0 crashed.")
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	t.Logf("follower2 recovered.")
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	t.Logf("Leader2 is elected.")
	// for _, server := range test.Clients {
	// 	state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
	// 	log.Print(state.MetaMap)
	// }

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// for _, server := range test.Clients {
	// 	state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
	// 	log.Print(state.MetaMap)
	// }

	// leader1 restored
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	t.Logf("Leader1 restored.")
	// for _, server := range test.Clients {
	// 	state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
	// 	log.Print(state.MetaMap)
	// }

	// leader2 send heartbeat
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// for id, server := range test.Clients {
	// 	state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
	// 	log.Print(id, state)
	// }

	for serverId := 0; serverId < len(test.Clients); serverId++ {
		state, err := test.Clients[serverId].GetInternalState(test.Context, &emptypb.Empty{})
		if err != nil {
			t.Fatalf("could not get internal state: " + err.Error())
		}
		log.Print(serverId, state.Log)
		if len(state.Log) != 2 {
			t.Fatalf(fmt.Sprintf("incorrect fileInfoMap length in Server%d", serverId))
		}
	}
}

func TestRaftLogsCorrectlyOverwritten(t *testing.T) {
	t.Logf("leader1 gets several requests while all other nodes are crashed. leader1 crashes. all other nodes are restored. leader2 gets a request. leader1 is restored.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// all other nodes are crashed
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// worker1 := InitDirectoryWorker("test0", SRC_PATH)
	// worker2 := InitDirectoryWorker("test1", SRC_PATH)
	// worker3 := InitDirectoryWorker("test2", SRC_PATH)
	// defer worker1.CleanUp()
	// defer worker2.CleanUp()
	// defer worker3.CleanUp()

	// //clients add different files
	// file1 := "multi_file1.txt"
	// file2 := "multi_file2.txt"
	// file3 := "multi_file3.txt"
	// err := worker1.AddFile(file1)
	// if err != nil {
	// 	t.FailNow()
	// }
	// err = worker2.AddFile(file2)
	// if err != nil {
	// 	t.FailNow()
	// }
	// err = worker3.AddFile(file3)
	// if err != nil {
	// 	t.FailNow()
	// }

	//client1 syncs
	// err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	// if err == nil {
	// 	t.Fatalf("Sync should have failed")
	// }
	// test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := test.Clients[0].UpdateFile(ctx, &surfstore.FileMetaData{
		Filename: "test.txt",
		Version:  1,
		BlockHashList: func() []string {
			blockHashList := make([]string, 0)
			blockHashList = append(blockHashList, "0")
			return blockHashList
		}(),
	})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err == nil {
		t.Fatalf("Sync should have failed")
	}
	// //client2 syncs
	// err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	// if err == nil {
	// 	t.Fatalf("Sync should have failed")
	// }
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = test.Clients[0].UpdateFile(ctx, &surfstore.FileMetaData{
		Filename: "test.txt",
		Version:  2,
		BlockHashList: func() []string {
			blockHashList := make([]string, 0)
			blockHashList = append(blockHashList, "0")
			return blockHashList
		}(),
	})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err == nil {
		t.Fatalf("Sync should have failed")
	}
	// time.Sleep(2 * time.Second)

	// leader1 crash and all other nodes are restored
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	// set leader2
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// client3 syncs
	// err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	// if err != nil {
	// 	t.Fatalf("Sync failed")
	// }
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = test.Clients[1].UpdateFile(ctx, &surfstore.FileMetaData{
		Filename: "test.txt",
		Version:  2,
		BlockHashList: func() []string {
			blockHashList := make([]string, 0)
			blockHashList = append(blockHashList, "0")
			return blockHashList
		}(),
	})
	// time.Sleep(2 * time.Second)
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Sync should not failed")
	}

	// leader1 restored and send heartbeat
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	// test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	for serverId := 0; serverId < len(test.Clients); serverId++ {
		state, err := test.Clients[serverId].GetInternalState(test.Context, &emptypb.Empty{})
		if err != nil {
			t.Fatalf("could not get internal state: " + err.Error())
		}
		t.Log(state)
		if len(state.Log) != 1 {
			t.Fatalf(fmt.Sprintf("incorrect fileInfoMap length in Server%d", serverId))
		}
	}
}

// func TestRaftSetLeader(t *testing.T) {
// 	//Setup
// 	cfgPath := "./config_files/3nodes.txt"
// 	test := InitTest(cfgPath)
// 	defer EndTest(test)

// 	// TEST
// 	leaderIdx := 0
// 	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

// 	// heartbeat
// 	for _, server := range test.Clients {
// 		server.SendHeartbeat(test.Context, &emptypb.Empty{})
// 	}

// 	for idx, server := range test.Clients {
// 		// all should have the leaders term
// 		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
// 		if state == nil {
// 			t.Fatalf("Could not get state")
// 		}
// 		if state.Term != int64(1) {
// 			t.Fatalf("Server %d should be in term %d", idx, 1)
// 		}
// 		if idx == leaderIdx {
// 			// server should be the leader
// 			if !state.IsLeader {
// 				t.Fatalf("Server %d should be the leader", idx)
// 			}
// 		} else {
// 			// server should not be the leader
// 			if state.IsLeader {
// 				t.Fatalf("Server %d should not be the leader", idx)
// 			}
// 		}
// 	}

// 	leaderIdx = 2
// 	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

// 	// heartbeat
// 	for _, server := range test.Clients {
// 		server.SendHeartbeat(test.Context, &emptypb.Empty{})
// 	}

// 	for idx, server := range test.Clients {
// 		// all should have the leaders term
// 		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
// 		if state == nil {
// 			t.Fatalf("Could not get state")
// 		}
// 		if state.Term != int64(2) {
// 			t.Fatalf("Server should be in term %d", 2)
// 		}
// 		if idx == leaderIdx {
// 			// server should be the leader
// 			if !state.IsLeader {
// 				t.Fatalf("Server %d should be the leader", idx)
// 			}
// 		} else {
// 			// server should not be the leader
// 			if state.IsLeader {
// 				t.Fatalf("Server %d should not be the leader", idx)
// 			}
// 		}
// 	}
// }

// func TestRaftBlockWhenMajorityDown(t *testing.T) {
// 	t.Logf("leader1 gets a request when the majority of the cluster is crashed.")
// 	cfgPath := "./config_files/3nodes.txt"
// 	test := InitTest(cfgPath)
// 	defer EndTest(test)
// 	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
// 	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	worker1 := InitDirectoryWorker("test0", SRC_PATH)
// 	worker2 := InitDirectoryWorker("test1", SRC_PATH)
// 	defer worker1.CleanUp()
// 	defer worker2.CleanUp()

// 	//clients add different files
// 	file1 := "multi_file1.txt"
// 	file2 := "multi_file1.txt"
// 	err := worker1.AddFile(file1)
// 	if err != nil {
// 		t.FailNow()
// 	}
// 	err = worker2.AddFile(file2)
// 	if err != nil {
// 		t.FailNow()
// 	}
// 	err = worker2.UpdateFile(file2, "update text")
// 	if err != nil {
// 		t.FailNow()
// 	}

// 	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
// 	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
// 	//client1 syncs
// 	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
// 	if err == nil {
// 		t.Fatalf("Sync failed")
// 	}
// }

// func TestRaftUpdateTwice(t *testing.T) {
// 	t.Logf("leader1 gets a request. leader1 gets another request.")
// 	cfgPath := "./config_files/3nodes.txt"
// 	test := InitTest(cfgPath)
// 	defer EndTest(test)
// 	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
// 	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	worker1 := InitDirectoryWorker("test0", SRC_PATH)
// 	worker2 := InitDirectoryWorker("test1", SRC_PATH)
// 	defer worker1.CleanUp()
// 	defer worker2.CleanUp()

// 	//clients add different files
// 	file1 := "multi_file1.txt"
// 	file2 := "multi_file2.txt"
// 	err := worker1.AddFile(file1)
// 	if err != nil {
// 		t.FailNow()
// 	}
// 	err = worker2.AddFile(file2)
// 	if err != nil {
// 		t.FailNow()
// 	}
// 	// err = worker2.UpdateFile(file2, "update text")
// 	// if err != nil {
// 	// 	t.FailNow()
// 	// }

// 	//client1 syncs
// 	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
// 	if err != nil {
// 		t.Fatalf("Sync failed")
// 	}
// 	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	//client2 syncs
// 	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
// 	if err != nil {
// 		t.Fatalf("Sync failed")
// 	}
// 	// time.Sleep(2 * time.Second)
// 	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	workingDir, _ := os.Getwd()

// 	//check client1
// 	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
// 	if err != nil {
// 		t.Fatalf("Could not find meta file for client1")
// 	}

// 	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
// 	if err != nil {
// 		t.Fatalf("Could not load meta file for client1")
// 	}
// 	if len(fileMeta1) != 1 {
// 		t.Fatalf("Wrong number of entries in client1 meta file")
// 	}
// 	if fileMeta1 == nil || fileMeta1[file1].Version != 1 {
// 		t.Fatalf("Wrong version for file1 in client1 metadata.")
// 	}

// 	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
// 	if e != nil {
// 		t.Fatalf("Could not read files in client base dirs.")
// 	}
// 	if !c {
// 		t.Fatalf("file1 should not change at client1")
// 	}

// 	//check client2
// 	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
// 	if err != nil {
// 		t.Fatalf("Could not find meta file for client2")
// 	}

// 	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
// 	if err != nil {
// 		t.Fatalf("Could not load meta file for client2")
// 	}
// 	if len(fileMeta2) != 2 {
// 		t.Fatalf("Wrong number of entries in client2 meta file")
// 	}
// 	if fileMeta2 == nil || fileMeta2[file1].Version != 1 {
// 		t.Fatalf("Wrong version for file1 in client2 metadata.")
// 	}

// 	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
// 	if e != nil {
// 		t.Fatalf("Could not read files in client base dirs.")
// 	}
// 	if !c {
// 		t.Fatalf("wrong file1 contents at client2")
// 	}

// 	for serverId := 0; serverId < len(test.Clients); serverId++ {
// 		state, err := test.Clients[serverId].GetInternalState(test.Context, &emptypb.Empty{})
// 		if err != nil {
// 			t.Fatalf("could not get internal state: " + err.Error())
// 		}
// 		if len(state.MetaMap.FileInfoMap) != 2 {
// 			t.Fatalf(fmt.Sprintf("incorrect fileInfoMap length in Server%d", serverId))
// 		}
// 	}
// }
