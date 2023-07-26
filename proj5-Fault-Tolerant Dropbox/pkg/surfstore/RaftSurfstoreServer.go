package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore
	ip        string
	ipList    []string
	serverId  int64

	commitIndex    int64
	pendingCommits []chan bool
	lastApplied    int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// fmt.Println("Inside Get File Info Map function..")

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	majorityAlive, _ := s.SendHeartbeat(ctx, empty)
	if !majorityAlive.Flag {
		fmt.Println("Majority not alive")
		return nil, ERR_SERVER_CRASHED
	}

	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	majorityAlive, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if !majorityAlive.Flag {
		fmt.Println("Majority not alive")
		return nil, ERR_SERVER_CRASHED
	}

	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	majorityAlive, _ := s.SendHeartbeat(ctx, empty)
	if !majorityAlive.Flag {
		fmt.Println("Majority not alive")
		return nil, ERR_SERVER_CRASHED
	}

	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// fmt.Println("Inside Update File MDT...")

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// fmt.Println("Appending pending commits to self log...")
	s.log = append(s.log, &op)
	committed_chan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed_chan)

	// fmt.Println("Attempting commit in parallel thread... ")
	go s.attemptCommit()
	success := <-committed_chan
	if success {
		s.lastApplied = s.commitIndex
		// fmt.Println("Succesfully attempted commit = majority available. Updating File.. ")
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	// fmt.Println("Majority not available. Returning error server crashed")
	return nil, ERR_SERVER_CRASHED
}

func (s *RaftSurfstore) attemptCommit() {
	target_idx := s.commitIndex + 1
	pending_idx := int64(len(s.pendingCommits) - 1)
	commit_chan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		// fmt.Println("Commiting entry..")
		go s.commitData(int64(idx), target_idx, commit_chan)
	}

	commitCount := 1
	for {
		// Crashed nodes
		commit := <-commit_chan
		s.isCrashedMutex.RLock()
		isCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()
		if isCrashed {
			s.pendingCommits[pending_idx] <- false
			break
		}

		if commit != nil && commit.Success {
			commitCount++
		}
		if commitCount > len(s.ipList)/2 {
			s.commitIndex = target_idx
			s.pendingCommits[pending_idx] <- true
			break
		}
	}
}

func (s *RaftSurfstore) commitData(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	for {
		s.isCrashedMutex.RLock()
		isCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()
		if isCrashed {
			commitChan <- &AppendEntryOutput{Success: false}
		}

		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)

		var prevLogTerm int64
		if entryIdx == 0 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[entryIdx-1].Term
		}
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: entryIdx - 1,
			PrevLogTerm:  prevLogTerm,
			Entries:      s.log[:entryIdx+1],
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)

		if output != nil {
			if output.Success {
				commitChan <- output
				return
			}
		}
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return output, ERR_SERVER_CRASHED
	}

	if input.Term > s.term {
		s.term = input.Term
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		return output, nil
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	for index, logEntry := range s.log {
		s.lastApplied = int64(index - 1)
		if len(input.Entries) < index+1 {
			s.log = s.log[:index]
			input.Entries = make([]*UpdateOperation, 0)
			break
		}
		if logEntry != input.Entries[index] {
			s.log = s.log[:index]
			input.Entries = input.Entries[index:]
			break
		}
		if len(s.log) == index+1 {
			input.Entries = input.Entries[index+1:]
		}
	}

	// 4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))

		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
	}
	output.Success = true
	return output, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	majorityAlive := false
	alive_count := 1
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return &Success{Flag: false}, nil
		}
		client := NewRaftSurfstoreClient(conn)

		var prevLogTerm int64
		if s.commitIndex == -1 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[s.commitIndex].Term
		}
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: s.commitIndex,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)
		if output != nil {
			alive_count++
			if alive_count > len(s.ipList)/2 {
				majorityAlive = true
			}
		}
	}
	return &Success{Flag: majorityAlive}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.term++
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
