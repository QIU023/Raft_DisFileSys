package surfstore

import (
	context "context"
	"log"
	"math/rand"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need

	status   string // leader, follower, candidate
	votedFor int64  // voted for who, -1 means not voted for anyone

	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	self_server_ID     int64
	group_id_addr_maps map[int]string

	commitIndex              int // init as 0
	nextIndex_for_follower   map[int]int
	matchIndex_for_followers map[int]int

	metaStoreMutex *sync.RWMutex
	logMutex       *sync.RWMutex

	// usable bool

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) printLogsAndState() {
	log.Print(s.self_server_ID)
	log.Print(s.log)
	log.Print(s.metaStore.FileMetaMap)
}

func (s *RaftSurfstore) ClusterBlockingWhenUnavailable(ctx context.Context) error {
	if s.isCrashed {
		return ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return ERR_NOT_LEADER
	}

	aliveChan := make(chan bool)
	go s.sendToAllFollowersInParallel(ctx, &aliveChan, false, true, false)

	// will block until getting all responses
	cluster_available := <-aliveChan
	log.Print(cluster_available)
	return nil
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.ClusterBlockingWhenUnavailable(ctx)
	return s.metaStore.GetFileInfoMap(ctx, empty)

}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.ClusterBlockingWhenUnavailable(ctx)
	return s.metaStore.GetBlockStoreMap(ctx, blockHashesIn)

}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.ClusterBlockingWhenUnavailable(ctx)
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	obj, ok := s.metaStore.FileMetaMap[filemeta.Filename]
	if ok {
		leader_ver := obj.Version
		if filemeta.Version-leader_ver != 1 {
			return &Version{Version: -1}, nil
		}
	}

	// after majority of followers approved the update, then leader update metadata
	s.logMutex.Lock()
	s.log = append(s.log, &UpdateOperation{Term: s.term, FileMetaData: filemeta})
	s.logMutex.Unlock()

	commitChan := make(chan bool)
	go s.sendToAllFollowersInParallel(ctx, &commitChan, true, true, false)

	// will block until getting majority, but if self crashed, will not commit
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		s.metaStoreMutex.Lock()
		s.logMutex.RLock()
		defer s.metaStoreMutex.Unlock()
		defer s.logMutex.RUnlock()
		for idx := s.commitIndex + 1; idx < len(s.log); idx++ {
			ver, err := s.metaStore.UpdateFile(ctx, s.log[idx].FileMetaData)
			if idx == len(s.log)-1 {
				s.commitIndex = len(s.log) - 1
				return ver, err
			}
		}
	}
	return &Version{Version: -1}, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context,
	out_chan *chan bool, send_log bool, block bool, forvote bool) {
	// send entry to all my followers and count the replies

	responses := make(chan bool, len(s.group_id_addr_maps)-1)
	// contact all the follower, send some AppendEntries call
	for idx, _ := range s.group_id_addr_maps {
		if int64(idx) == s.self_server_ID {
			continue
		}
		if forvote {
			go s.RequestVoteFromOther(ctx, idx, responses)
		} else {
			go s.SendEntryToFollower(ctx, idx, responses, send_log, block)
		}
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if s.isCrashed {
			*out_chan <- false
			return
		}
		if block {
			if totalAppends > len(s.group_id_addr_maps)/2 || totalResponses == len(s.group_id_addr_maps) {
				break
			}
		} else {
			if totalResponses == len(s.group_id_addr_maps) {
				break
			}
		}
	}

	*out_chan <- totalAppends > len(s.group_id_addr_maps)/2
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	if s.isCrashed {
		// crashed follower, need retry
		return nil, ERR_SERVER_CRASHED
	}

	reject_output := &AppendEntryOutput{
		ServerId:     int64(s.self_server_ID),
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}

	if input.Term < s.term {
		// invalid out-of-date update, do not retry
		return reject_output, nil
	} else {
		if s.isLeader {
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()
		} else if s.status == "candidate" {
			if input.Term > s.term {
				s.status = "follower"
			} else {
				return reject_output, nil
			}
		}
		s.term = input.Term
		// log.Print(s.term, input.Term)
		succ_output := &AppendEntryOutput{
			ServerId:     int64(s.self_server_ID),
			Term:         s.term,
			Success:      true,
			MatchedIndex: -1,
		}
		if len(input.Entries) > 0 {
			// update log
			if input.PrevLogIndex >= 0 {
				s.logMutex.RLock()
				if input.PrevLogIndex >= int64(len(s.log)) || s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
					// invalid out-of-date update, do not retry
					s.logMutex.RUnlock()
					return reject_output, nil
				} else {
					// update log
					s.logMutex.Lock()
					s.log = s.log[:input.PrevLogIndex+1]
					s.log = append(s.log, input.Entries...)
					succ_output.MatchedIndex = int64(len(s.log) - 1)
					s.logMutex.Unlock()
				}
				s.logMutex.RUnlock()
			} else {
				// input.PrevLogIndex == -1
				// means the leader has no log, but follower has log
				// so reject the update
				return reject_output, nil
			}

			// s.logMutex.Lock()
			// s.log = input.Entries
			// succ_output.MatchedIndex = int64(len(s.log) - 1)
			// s.logMutex.Unlock()
		}

		// commit for those that have been committed by leader, but not yet committed by self
		if input.LeaderCommit > int64(s.commitIndex) && len(s.log) > 0 {
			var new_commitIndex int
			if (int)(input.LeaderCommit) <= len(s.log)-1 {
				// some logs in incoming entries haven't been committed by leader
				new_commitIndex = (int)(input.LeaderCommit)
			} else {
				// all logs in incoming entries have been committed by leader
				new_commitIndex = len(s.log) - 1
			}
			s.metaStoreMutex.Lock()
			s.logMutex.RLock()
			defer s.metaStoreMutex.Unlock()
			defer s.logMutex.RUnlock()
			for it := s.commitIndex + 1; it <= new_commitIndex; it++ {
				update_filemeta := s.log[it].FileMetaData
				s.metaStore.UpdateFile(ctx, update_filemeta)
			}
			s.commitIndex = new_commitIndex
		}
		return succ_output, nil
	}
}

func (s *RaftSurfstore) RequestVote(ctx context.Context, input *RequestVoteInput) (*RequestVoteOutput, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if input.Term < s.term {
		return &RequestVoteOutput{
			Term:        s.term,
			VoteGranted: false,
		}, nil
	} else {
		if s.isLeader {
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()
		}
		if s.votedFor != -1 && s.votedFor != input.CandidateId {
			// already voted for someone else in this term
			return &RequestVoteOutput{
				Term:        s.term,
				VoteGranted: false,
			}, nil
		} else {
			// vote for the candidate
			s.term = input.Term
			s.votedFor = input.CandidateId
			return &RequestVoteOutput{
				Term:        s.term,
				VoteGranted: true,
			}, nil
		}
	}
}

func (s *RaftSurfstore) LaunchElection(ctx context.Context, _ *emptypb.Empty) error {
	if s.isCrashed {
		return ERR_SERVER_CRASHED
	}

	s.term += 1
	voteChan := make(chan bool)
	go s.sendToAllFollowersInParallel(ctx, &voteChan, false, false, true)
	// may be timeout because others majority crashed
	request_result := <-voteChan
	if request_result {
		// win the election
		_, err := s.SetLeader(ctx, &emptypb.Empty{})
		checkError(err)
	} else {
		// do not win the election, maybe timeout, or others won
		for {
			if s.status == "follower" {
				// others won
				break
			}
		}
	}
	return nil
}

func (s *RaftSurfstore) OuterPeriodlyLaunchElection() error {
	// if others won, self status -> follower, end calling this RPC, if no one wins, keeping calling this RPC
	// this RPC should be called by goroutine!!!
	s.status = "candidate"

	// rand_time := 100 + rand.Int()%100
	rand.Seed(time.Now().UnixNano())
	// Generate a random delay between 100-200ms
	delay := rand.Intn(101) + 100
	// x := rand.Int31n(rand_time * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(delay)*time.Millisecond)
	defer cancel()

	for {
		// continually election, until someone won, nobody won, then RPC will timeout error, this goroutine is trapped in loop
		err := s.LaunchElection(ctx, &emptypb.Empty{})
		if err == nil {
			return nil
		}
		if s.isCrashed {
			return ERR_SERVER_CRASHED
		}
	}
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	time.Sleep(500 * time.Millisecond)
	// s.term += 1
	s.isLeader = true
	s.status = "leader"
	for id, _ := range s.group_id_addr_maps {
		if int64(id) == s.self_server_ID {
			continue
		}
		s.nextIndex_for_follower[id] = len(s.log)
		s.matchIndex_for_followers[id] = 0
	}
	commitChan := make(chan bool)
	// Heartbeat
	go s.sendToAllFollowersInParallel(ctx, &commitChan, false, false, false)

	// will block until getting majority
	// commit := <-commitChan
	s.isLeaderMutex.Unlock()
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) RequestVoteFromOther(ctx context.Context, other_id int,
	responses chan bool) (*RequestVoteOutput, error) {

	conn, err := grpc.Dial(s.group_id_addr_maps[other_id], grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c := NewRaftSurfstoreClient(conn)
	defer conn.Close()

	requestinput := &RequestVoteInput{
		Term:         s.term,
		CandidateId:  s.self_server_ID,
		LastLogIndex: int64(len(s.log) - 1),
		LastLogTerm:  s.log[len(s.log)-1].Term,
	}
	requestoutput, err := c.RequestVote(ctx, requestinput)
	if err != nil {
		// other server crashed or RPC timeout
		responses <- false
		return nil, err
	}
	responses <- requestoutput.VoteGranted
	return requestoutput, nil
}

func (s *RaftSurfstore) SendEntryToFollower(ctx context.Context, follower_id int,
	responses chan bool, send_log bool, block bool) (*AppendEntryOutput, error) {
	conn, err := grpc.Dial(s.group_id_addr_maps[follower_id], grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c := NewRaftSurfstoreClient(conn)

	defer conn.Close()

	EntryInput := &AppendEntryInput{
		Term:         s.term,
		LeaderCommit: int64(s.commitIndex),
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
	}
	if send_log {
		// for follower_last_idx := s.nextIndex_for_follower[follower_id]; follower_last_idx >= s.matchIndex_for_followers[follower_id]; follower_last_idx-- {
		// iter to find last log entry that exist in this follower
		// EntryInput.Entries = s.log[follower_last_idx:]
		// EntryInput.PrevLogIndex = int64(follower_last_idx)
		// if follower_last_idx < len(s.log) {
		// 	EntryInput.PrevLogTerm = s.log[follower_last_idx].Term
		// }
		// retry to deal with crashed followers
		s.logMutex.RLock()
		EntryInput.Entries = s.log
		s.logMutex.RUnlock()
		for {
			if s.isCrashed {
				responses <- false
				return nil, ERR_SERVER_CRASHED
			}
			EntryOutput, err := c.AppendEntries(ctx, EntryInput)
			if err == nil {
				// follower is not crashed
				if !EntryOutput.Success {
					if EntryOutput.Term > EntryInput.Term {
						// another leader is born
						responses <- false
						return EntryOutput, err
					}
				} else {
					// follower approved
					responses <- true
					// if send_log {
					// 	s.matchIndex_for_followers[follower_id] = int(EntryOutput.MatchedIndex)
					// }
					return EntryOutput, nil
				}
			} else {
				// crashed follower
				if !block {
					responses <- false
					return nil, ERR_SERVER_CRASHED
				} else {
					time.Sleep(250 * time.Millisecond)
				}
			}
		}
	} else {
		// sending heartbeat
		EntryInput.Entries = make([]*UpdateOperation, 0)
		for {
			if s.isCrashed {
				responses <- false
				return nil, ERR_SERVER_CRASHED
			}
			EntryOutput, err := c.AppendEntries(ctx, EntryInput)
			if err != nil {
				// not approved
				if EntryOutput == nil {
					// follower is crashed
					if !block {
						responses <- false
						return nil, ERR_SERVER_CRASHED
					} else {
						time.Sleep(250 * time.Millisecond)
					}
				} else {
					log.Fatal("unexpected for not nil err and non nil EntryOutput, since sending heartbeat here")
				}
			} else {
				// follower approved
				responses <- true
				return EntryOutput, nil
			}
		}
	}
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false

// Sends a round of AppendEntries to all other nodes. The leader will attempt to replicate logs to all other nodes when this is called. It can be called even when there are no entries to replicate. If a node is not in the leader state it should do nothing.
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	aliveChan := make(chan bool)
	go s.sendToAllFollowersInParallel(ctx, &aliveChan, true, false, false)

	// will block until getting all responses
	cluster_available := <-aliveChan

	return &Success{Flag: cluster_available}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(context.Background(), empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
