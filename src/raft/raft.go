package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/debug"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state            State
	applyCh          chan ApplyMsg
	internalApplyCh  chan *LogEntry
	startRateLimiter chan int

	currentTerm int
	votedFor    int // peer's index (-1 means no vote)
	log         Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	snapshot    []byte

	lastLeaderPing      time.Time         // used for election timeout
	lastInstanceContact map[int]time.Time // used for sending AppendEntry RPC
	watchdogChannels    map[int]chan int

	// Control channel for notifying the main goroutine. Only the main goroutine should read from this.
	controlCh chan State
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	if rf.log.HaveSnapshot {
		rf.persister.Save(raftstate, rf.snapshot)
	} else {
		rf.persister.Save(raftstate, nil)
	}

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curTerm int
	var votedFor int
	var log Log
	if d.Decode(&curTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		err := "Error: reading persisted state."
		debug.Debug(debug.DError, rf.me, err)
		panic(err)
	} else {
		rf.mu.Lock()
		rf.currentTerm = curTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	debug.Debug(debug.DSnap, rf.me, "Create a new snapshot with index:%v.", index)
	rf.snapshot = snapshot
	if entry, ok := rf.log.Get(index); ok {
		rf.log.InstallSnapshot(entry.Index, entry.Term)
	} else {
		panic(fmt.Sprintf("Error: (Snapshot) could not find entry with index:%v.", index))
	}
	rf.log.DeleteTo(index, rf.me)
	rf.persist()
	debug.Debug(debug.DSnap, rf.me, "Snapshot with index:%v Completed.", index)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	debug.Debug(debug.DRPC, rf.me, "InstallSnapshot from %v. Term:%v, SnapshotIndex:%v, SnapshotTerm:%v.",
		args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()

	if rf.currentTerm > args.Term {
		debug.Debug(debug.DTerm, rf.me, "Old Term (%v < %v), InstallSnapshot rejected.", args.Term, rf.currentTerm)
		return
	} else if rf.currentTerm < args.Term {
		// Fallback to Follower
		debug.Debug(debug.DTerm, rf.me, "Discovered greater Term (%v > %v)", args.Term, rf.currentTerm)
		DiscoverHigherTerm(rf, args.Term)
		rf.persist()
		return
	}

	debug.Debug(debug.DSnap, rf.me, "Received valid snapshot with Index:%v, Term:%v.",
		args.LastIncludedIndex, args.LastIncludedTerm)

	entry, ok := rf.log.Get(args.LastIncludedIndex)

	// TODO: What if this log is covered by our own snapshot?
	if ok && entry.Term == args.LastIncludedTerm {
		debug.Debug(debug.DSnap, rf.me, "Trimming log up to Index: %v.", args.LastIncludedIndex)
		rf.log.DeleteTo(args.LastIncludedIndex, rf.me)
	} else if !ok && rf.log.HaveSnapshot && args.LastIncludedIndex <= rf.log.SnapshotLastIndex {
		// Old InstallSnapshot
		debug.Debug(debug.DSnap, rf.me, "Old snapshot. LastIncludedTerm:%v, SnapshotLastIndex:%v.",
			args.LastIncludedTerm, rf.log.SnapshotLastIndex)
		return
	} else {
		debug.Debug(debug.DSnap, rf.me, "Drooping all log entries.")
		rf.log.DeleteAll(rf.me)
	}
	rf.snapshot = args.Snapshot
	rf.log.InstallSnapshot(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persist()

	// apply snapshot
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.log.SnapshotLastTerm,
		SnapshotIndex: rf.log.SnapshotLastIndex,
		CommandValid:  false,
	}

	debug.Debug(debug.DSnap, rf.me, "Installed snapshot.")

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	if args.Term != rf.currentTerm || rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	debug.Debug(debug.DRPC, rf.me, "Sending InstallSnapshot to %v.", server)
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply); !ok {
		return false
	}
	debug.Debug(debug.DRPC, rf.me, "Received InstallSnapshot response from %v.", server)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm || rf.state != Leader || rf.killed() {
		debug.Debug(debug.DConsist, rf.me, "Inconsistent InstallSnapshot response. args.Term:%v, curTerm:%v, state:%v, isKilled:%v",
			args.Term, rf.currentTerm, rf.state, rf.killed())
		return false
	}

	if !rf.log.HaveSnapshot {
		debug.Debug(debug.DConsist, rf.me, "Invalid reply for a leader that has not a snapshot.")
		return false
	}

	// Check if there are greater Terms
	if reply.Term > rf.currentTerm {
		debug.Debug(debug.DTerm, rf.me, "Discovered greater Term (%v > %v)", reply.Term, rf.currentTerm)

		// Fallback to Follower state
		DiscoverHigherTerm(rf, reply.Term)
		return false
	}

	// TODO: check if this is necessary
	debug.Debug(debug.DLog, rf.me, "Update nextIndex of %v: %v --> %v.", server,
		rf.nextIndex[server], rf.log.SnapshotLastIndex+1)
	rf.nextIndex[server] = rf.log.SnapshotLastIndex + 1

	go appendEntriesWrapper(rf, server)

	return true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	debug.Debug(debug.DRPC, rf.me, "RequestVote from %v", args.CandidateId)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		debug.Debug(debug.DTerm, rf.me, "Old Term (%v < %v), vote rejected.", args.Term, rf.currentTerm)
		return
	} else if rf.currentTerm < args.Term {
		// Fallback to Follower
		debug.Debug(debug.DTerm, rf.me, "Discovered greater Term (%v > %v)", args.Term, rf.currentTerm)
		DiscoverHigherTerm(rf, args.Term)
		rf.votedFor = -1
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if CheckLogIsUpToDate(rf, args.LastLogTerm, args.LastLogIndex) >= 0 {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			ResetElectionTimer(rf)
			debug.Debug(debug.DVote, rf.me, "Vote granted to %v.", args.CandidateId)
		} else {
			reply.VoteGranted = false
			lastIndex, lastTerm := rf.log.GetLastIndexTerm()
			debug.Debug(debug.DConsist, rf.me, "Log is not up-to-date (lastEntries: candidate:(%v, %v), me:(%v, %v)), vote rejected.",
				args.LastLogIndex, args.LastLogTerm, lastIndex, lastTerm)
		}
	} else {
		reply.VoteGranted = false
		debug.Debug(debug.DVote, rf.me, "Voted another instance (%v), vote rejected for %v.",
			rf.votedFor, args.CandidateId)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan int) bool {
	rf.mu.Lock()
	if args.Term != rf.currentTerm || rf.state != Candidate || rf.killed() {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	debug.Debug(debug.DRPC, rf.me, "Sending RequestVote to %v.", server)
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return false
	}
	debug.Debug(debug.DRPC, rf.me, "Received RequestVote response from %v.", server)

	// Caller procedure
	rf.mu.Lock()
	//defer rf.mu.Unlock()

	if args.Term != rf.currentTerm || rf.state != Candidate || rf.killed() {
		debug.Debug(debug.DConsist, rf.me, "Inconsistent RequestVote response. args.Term:%v, curTerm:%v, state:%v, isKilled:%v",
			args.Term, rf.currentTerm, rf.state, rf.killed())
		rf.mu.Unlock()
		return false
	}

	// Check if there are greater Terms
	if reply.Term > rf.currentTerm {
		debug.Debug(debug.DTerm, rf.me, "Discovered greater Term (%v > %v)", reply.Term, rf.currentTerm)

		// Fallback to Follower state
		DiscoverHigherTerm(rf, reply.Term)
		rf.mu.Unlock()
		return false
	}

	if reply.VoteGranted {
		// Check consistency
		if reply.Term == rf.currentTerm && rf.state == Candidate {
			// Count vote.
			debug.Debug(debug.DVote, rf.me, "Valid vote from %v.", server)
			rf.mu.Unlock()

			// Send vote to voteCounter
			ch <- server
			debug.Debug(debug.DInfo, rf.me, "Sent Vote from %v to voteCounter.", server)

			return true
		} else {
			debug.Debug(debug.DConsist, rf.me, "Vote from %v consistency check failed. vote:%+v, curTerm:%v, state:%v",
				server, reply, rf.currentTerm, rf.state)
			rf.mu.Unlock()
			return false
		}
	}

	debug.Debug(debug.DVote, rf.me, "Vote not granted from %v.", server)
	rf.mu.Unlock()
	return false
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PervLogIndex int
	PervLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	XTerm   int
	XIndex  int
	XLen    int
	Success bool
}

/*
Requires Lock.
*/
func updateCommitIndex(rf *Raft, leaderCommit int) {
	lastLogIndex, _ := rf.log.GetLastIndexTerm()

	newCommitIndex := min(leaderCommit, lastLogIndex)
	if newCommitIndex > rf.commitIndex {
		debug.Debug(debug.DCommit, rf.me, "Updating commitIndex: %v --> %v.", rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
	}

}

// Requires Lock.
func updateLastApplied(rf *Raft) {
	if rf.commitIndex > rf.lastApplied {
		oldLastApplied := rf.lastApplied
		updateCount := rf.commitIndex - rf.lastApplied
		for i := 0; i < updateCount; i++ {
			if rf.lastApplied >= rf.commitIndex {
				break
			}
			rf.lastApplied += 1
			if rf.log.HaveSnapshot && rf.lastApplied <= rf.log.SnapshotLastIndex {
				continue
			}
			entry, ok := rf.log.Get(rf.lastApplied)
			if !ok {
				err := fmt.Sprintf("Error: (updateLastApplied) No entry at index:%v.", rf.lastApplied)
				debug.Debug(debug.DError, rf.me, err)
				panic(err)
			}
			rf.internalApplyCh <- entry
		}
		debug.Debug(debug.DCommit, rf.me, "Updated lastApplied: %v --> %v.",
			oldLastApplied, rf.lastApplied)
	}

}

/*
This function implements two goroutines. One is responsible for receiving new entries to apply from
AppendEntries handler and queueing them (it won't block in any situation). Other one is responsible
for reading from the internal queue of entries and applying them (sending them through the applyCh)
in order (this goroutine might block because the upper-level service might not read from it at all times.
However, this won't pose any challenges to the functionality of Raft since we have not blocked AppendEntries
handler and the lock is free)
*/
func (rf *Raft) ApplierAgent() {
	entries := make([]*LogEntry, 0)
	var mu sync.Mutex
	ch := make(chan int)

	/* Goroutine responsible for receiving new entries to apply from
	AppendEntries handler and queueing them */
	go func() {
		for !rf.killed() {
			e := <-rf.internalApplyCh
			mu.Lock()
			entries = append(entries, e)
			mu.Unlock()
			select {
			case ch <- 0:
			default:
			}

		}
	}()

	// Code responsible for reading from the internal queue of entries and applying them
	for {
		if rf.killed() {
			return
		}
		mu.Lock()
		for len(entries) < 1 {
			mu.Unlock()
			<-ch
			mu.Lock()
		}
		e := entries[0]
		entries = entries[1:]
		mu.Unlock()
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      e.Command,
			CommandIndex: e.Index,
		}
		debug.Debug(debug.DCommit, rf.me, "Applied index:%v.", e.Index)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	debug.Debug(debug.DRPC, rf.me, "AppendEntries from %v. args:%+v", args.LeaderId, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()

	if args.Term < rf.currentTerm {
		debug.Debug(debug.DTerm, rf.me, "Old Term, AppendEntries rejected.")
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		debug.Debug(debug.DTerm, rf.me, "Discovered greater Term (%v > %v)", args.Term, rf.currentTerm)
		DiscoverHigherTerm(rf, args.Term)
	} else if rf.state != Follower {
		debug.Debug(debug.DVote, rf.me, "Discovered a leader at %v state.", rf.state)
		DiscoverHigherTerm(rf, args.Term)
	}

	// So far, we are sure that the leader is valid, so consider this RPC as a Heartbeat.
	//debug.Debug(debug.DTimer, rf.me, "Resetting election timer.")
	ResetElectionTimer(rf)

	var entry *LogEntry
	var ok bool
	if args.PervLogIndex == 0 && args.PervLogTerm == 0 {
		entry = &LogEntry{
			Term:  0,
			Index: 0,
		}
		ok = true
	} else if rf.log.HaveSnapshot && args.PervLogIndex == rf.log.SnapshotLastIndex &&
		args.PervLogTerm == rf.log.SnapshotLastTerm {
		entry = &LogEntry{
			Term:  rf.log.SnapshotLastTerm,
			Index: rf.log.SnapshotLastIndex,
		}
		ok = true
	} else {
		entry, ok = rf.log.Get(args.PervLogIndex)
	}

	// If you don't have an entry at PervLogIndex, return false
	if !ok {
		if rf.log.HaveSnapshot && args.PervLogIndex <= rf.log.SnapshotLastIndex {
			panic(fmt.Sprintf("Error: (AppendEntries) Invalid entry access with snapshot. SnapshotLastIndex:%v, PervLogIndex:%v",
				rf.log.SnapshotLastIndex, args.PervLogIndex))
		}
		debug.Debug(debug.DLog, rf.me, "Does not have entry at index:%v. (Log len:%v)",
			args.PervLogIndex, rf.log.Len())
		reply.Success = false
		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = rf.log.Len()
		updateCommitIndex(rf, args.LeaderCommit)
		return
	}

	// Consistency check for Previous Log Entry
	if entry.Term != args.PervLogTerm {
		debug.Debug(debug.DConsist, rf.me, "Term of entry at index:%v is inconsistent. (self:%v, other:%v)",
			args.PervLogIndex, entry.Term, args.PervLogTerm)
		reply.XIndex = rf.log.FirstIndexOfTerm(entry.Term)
		reply.Success = false
		reply.XTerm = entry.Term
		if reply.XIndex == -1 {
			// Panic due to undesired state.
			rf.log.PrintAll()
			panic(fmt.Sprintf("Error: Could not find first index for term:%v.", entry.Term))
		}
		rf.log.DeleteFrom(entry.Index, rf.me)
		rf.persist()

		updateCommitIndex(rf, args.LeaderCommit)
		return
	} else {
		debug.Debug(debug.DLog, rf.me, "Log is consistent with leader. Sending success.")
		reply.Success = true
	}

	// Appending new entries
	for _, e := range args.Entries {
		curEntry, ok := rf.log.Get(e.Index)
		if !ok {
			// We don't have the entry, append it.
			rf.log.Add(e.Command, e.Term, rf.me)
			rf.persist()
		} else {
			// Consistency check
			if curEntry.Term != e.Term {
				rf.log.DeleteFrom(e.Index, rf.me)
				rf.log.Add(e.Command, e.Term, rf.me)
				rf.persist()
			}
		}
	}

	updateCommitIndex(rf, args.LeaderCommit)
	updateLastApplied(rf)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	debug.Debug(debug.DRPC, rf.me, "Sending AppendEntries to %v.", server)
	rf.mu.Lock()
	if rf.currentTerm != args.Term || rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return false
	}
	debug.Debug(debug.DRPC, rf.me, "Received AppendEntries response from %v.", server)

	// Caller procedure
	rf.mu.Lock()

	// Consistency checks
	if args.Term != rf.currentTerm {
		debug.Debug(debug.DConsist, rf.me, "Inconsistent current and args term in AppendEntries handler.")
		rf.mu.Unlock()
		return false
	}

	if rf.killed() || rf.state != Leader {
		debug.Debug(debug.DConsist, rf.me, "Consistency check failed. isKilled:%v, state:%v",
			rf.killed(), rf.state)
		rf.mu.Unlock()
		return false
	}

	// Check if there are greater Terms
	if reply.Term > rf.currentTerm {
		debug.Debug(debug.DTerm, rf.me, "Discovered greater Term (%v > %v)", reply.Term, rf.currentTerm)

		// Fallback to Follower state
		DiscoverHigherTerm(rf, reply.Term)
		rf.mu.Unlock()
		return false
	} else if reply.Term < rf.currentTerm {
		debug.Debug(debug.DConsist, rf.me, "Received AppendEntries response having old term (cur:%v, response:%v).",
			rf.currentTerm, reply.Term)
		rf.mu.Unlock()
		return false
	}

	// Updating last contact.
	debug.Debug(debug.DInfo, rf.me, "Updating %v last contact.", server)
	rf.lastInstanceContact[server] = time.Now()

	// Update nextIndex and matchIndex
	if reply.Success {
		var newMatchIndex int
		if len(args.Entries) < 1 {
			newMatchIndex = args.PervLogIndex
		} else {
			newMatchIndex = args.Entries[len(args.Entries)-1].Index
		}
		if newMatchIndex > rf.matchIndex[server] {
			debug.Debug(debug.DLog, rf.me, "Update matchIndex for %v: %v --> %v.",
				server, rf.matchIndex[server], newMatchIndex)
			rf.matchIndex[server] = newMatchIndex
		}

		if len(args.Entries) >= 1 {
			lastEntry := args.Entries[len(args.Entries)-1]
			if lastEntry.Index+1 > rf.nextIndex[server] {
				debug.Debug(debug.DLog, rf.me, "Update nextIndex for %v: %v --> %v.",
					server, rf.nextIndex[server], lastEntry.Index+1)
				rf.nextIndex[server] = lastEntry.Index + 1
			}

		}

	} else {
		oldNextIndex := rf.nextIndex[server]
		if reply.XIndex == -1 && reply.XTerm == -1 {
			if reply.XLen < 1 {
				rf.nextIndex[server] = 1
			} else {
				rf.nextIndex[server] = reply.XLen
			}
		} else if !rf.log.HasTerm(reply.XTerm) {
			rf.nextIndex[server] = reply.XIndex
		} else {
			if last := rf.log.LastIndexOfTerm(reply.XTerm); last != -1 {
				rf.nextIndex[server] = last
			} else {
				if reply.XLen < 1 {
					rf.nextIndex[server] = 1
				} else {
					rf.nextIndex[server] = reply.XLen
				}
			}

		}
		if oldNextIndex != rf.nextIndex[server] {
			debug.Debug(debug.DLog, rf.me, "Decrement nextIndex for %v: %v --> %v.",
				server, oldNextIndex, rf.nextIndex[server])
		}
		if rf.nextIndex[server] < 1 {
			err := fmt.Sprintf("Error: Invalid nextIndex:%v for server:%v.", rf.nextIndex[server], server)
			debug.Debug(debug.DError, rf.me, err)
			panic(err)
		}
	}

	// Update commitIndex
	updateLeaderCommitIndex(rf)

	// Apply new commits
	updateLastApplied(rf)

	// Notify the watchdog
	ch := rf.watchdogChannels[server]
	rf.mu.Unlock()
	//debug.Debug(debug.DLeader, rf.me, "Sending notification to %v's watchdog.", server)
	var flag int
	if len(args.Entries) == 0 {
		flag = 1
	} else {
		flag = 0
	}
	ch <- flag
	//debug.Debug(debug.DLeader, rf.me, "Notified %v's watchdog.", server)

	return true
}

/*
Note: Updating by counting index should only be applied to entries with current term.
*/
func updateLeaderCommitIndex(rf *Raft) {
	lastEntry := rf.log.GetLast()
	var candidateIndex int

	// Return if the log is empty
	if lastEntry != nil {
		candidateIndex = lastEntry.Index
	} else {
		return
	}

	for {
		entry, ok := rf.log.Get(candidateIndex)
		if !ok {
			break
		}
		// Only count replicas if the term == currentTerm
		if entry.Term != rf.currentTerm {
			break
		}
		// Check for the for updating.
		if candidateIndex <= rf.commitIndex {
			return
		}
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= candidateIndex {
				count += 1
			}
		}
		if count > len(rf.peers)/2 {
			debug.Debug(debug.DCommit, rf.me, "Update commitIndex: %v --> %v.",
				rf.commitIndex, candidateIndex)
			rf.commitIndex = candidateIndex
			return
		}
		candidateIndex -= 1
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	debug.Debug(debug.DClient, rf.me, "Received an new command:%v.", command)

	if rf.state != Leader {
		debug.Debug(debug.DClient, rf.me, "Cannot accept a command at %v state", rf.state)
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}

	// Adding command to the log.
	rf.log.Add(command, rf.currentTerm, rf.me)
	rf.persist()

	lastEntry := rf.log.GetLast()

	rf.mu.Unlock()

	go func() {
		rf.startRateLimiter <- 0
	}()

	return lastEntry.Index, lastEntry.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*
Timer for leader election. It is safe to use it even in Leader state (In this case,
it won't ever cause timeout)
*/
func (rf *Raft) ticker() {
	flag := false
	var electionTimeout int64

	for !rf.killed() {
		if !flag {
			electionTimeout = 500 + (rand.Int63() % 300)
			flag = true
		}

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != Leader {
			if time.Duration(time.Since(rf.lastLeaderPing)).Milliseconds() > electionTimeout {
				debug.Debug(debug.DTimer, rf.me, "Election timeout.")
				debug.Debug(debug.DState, rf.me, "State change: %v --> Candidate.", rf.state)
				rf.state = Candidate
				flag = false
				//ResetElectionTimer(rf)

				// Notify main goroutine
				// debug.Debug(debug.DInfo, rf.me, "Sending notification to main.")
				rf.controlCh <- Candidate
			}
		}

		rf.mu.Unlock()

		// Currently, our timestep to check the election timer is 10 ms.
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

/*
This function starts several goroutines responsible for election, so it does not block and returns promptly.
*/
func startElection(rf *Raft) {
	ch := make(chan int)
	result := make(map[int]int)

	rf.mu.Lock()
	// Starting election
	rf.currentTerm += 1
	result[rf.me] = 0
	rf.votedFor = rf.me
	ResetElectionTimer(rf)
	rf.persist()

	lastLogIndex, lastLogTerm := rf.log.GetLastIndexTerm()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply, ch)
		}
	}
	go voteCounter(rf, result, ch, rf.currentTerm)
	rf.mu.Unlock()
}

func voteCounter(rf *Raft, result map[int]int, ch chan int, term int) {
	for {
		// check if election is still going on.
		rf.mu.Lock()
		if rf.killed() || rf.state != Candidate || rf.currentTerm != term {
			debug.Debug(debug.DConsist, rf.me, "No election. Exiting from voteCounter.")
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		timeout := time.After(1 * time.Second)

		select {
		case <-timeout:
			debug.Debug(debug.DConsist, rf.me, "timeout of voteCounter.")
		case vote := <-ch:
			debug.Debug(debug.DInfo, rf.me, "Received a vote from %v at voteCounter.", vote)
			rf.mu.Lock()

			// Checking consistency of vote
			if rf.currentTerm == term && rf.state == Candidate && !rf.killed() {
				if _, ok := result[vote]; !ok {
					debug.Debug(debug.DInfo, rf.me, "Counting vote from %v.", vote)
					result[vote] = 0
				} else {
					debug.Debug(debug.DInfo, rf.me, "Duplicate vote from %v.", vote)
				}
				if len(result) > len(rf.peers)/2 {
					// Won the election
					debug.Debug(debug.DState, rf.me, "State change: %v --> Leader.", rf.state)

					rf.state = Leader

					// Notifying state transition
					rf.controlCh <- Leader
					rf.mu.Unlock()
					return
				}
			} else {
				debug.Debug(debug.DConsist, rf.me, "Inconsistent vote. isKilled:%v, curTerm:%v (election term:%v), state:%v",
					rf.killed(), rf.currentTerm, term, rf.state)
			}
			rf.mu.Unlock()
		}
	}
}

/*
Kicks off watchdogs required for sending AppendEntry RPCs to Followers. It doesn't block.
*/
func startLeader(rf *Raft) {
	rf.mu.Lock()

	lastLogIndex, _ := rf.log.GetLastIndexTerm()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}
	}
	curTerm := rf.currentTerm
	go StartRateLimiter(rf, curTerm)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			ch := make(chan int)
			rf.watchdogChannels[i] = ch
		}
	}

	// go leaderTimeoutCheck(rf, rf.currentTerm)

	rf.mu.Unlock()
	debug.Debug(debug.DInfo, rf.me, "Starting watchdogs ...")
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go instanceWatchDog(i, rf, curTerm)
		}
	}
	// TODO: Replace this no-op command with initial heartbeat.
	/* // Committing a no-op command upon winning the election.
	go rf.Start("no-op") */
}

func appendEntriesWrapper(rf *Raft, server int) bool {
	rf.mu.Lock()

	index := rf.nextIndex[server]

	var pervLogIndex int
	var pervLogTerm int
	pervEntry, ok := rf.log.Get(index - 1)
	if ok {
		pervLogIndex = pervEntry.Index
		pervLogTerm = pervEntry.Term
	} else {
		if !rf.log.HaveSnapshot {
			// Log is empty

			pervLogIndex = 0
			pervLogTerm = 0
		} else if rf.log.SnapshotLastIndex == index-1 {
			pervLogIndex = rf.log.SnapshotLastIndex
			pervLogTerm = rf.log.SnapshotLastTerm

		} else if rf.log.SnapshotLastIndex > index-1 {
			// Log is trimmed, so send InstallSnapshot

			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.SnapshotLastIndex,
				LastIncludedTerm:  rf.log.SnapshotLastTerm,
				Snapshot:          rf.snapshot,
			}
			reply := InstallSnapshotReply{}
			go rf.sendInstallSnapshot(server, &args, &reply)
			rf.mu.Unlock()
			return false
		} else {
			panic(fmt.Sprintf(
				"Error: (appendEntriesWrapper) Previous entry not found and index:%v is larger than SnapshotLastIndex: %v",
				index-1, rf.log.SnapshotLastIndex))
		}

	}

	entries := []LogEntry{}

	nextIndex := index
	for {
		entry, ok := rf.log.Get(nextIndex)
		if ok {
			entries = append(entries, *entry)
			nextIndex += 1
		} else {
			break
		}
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PervLogIndex: pervLogIndex,
		PervLogTerm:  pervLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	return rf.sendAppendEntries(server, &args, &reply)
}

/*
This function implements AppendEntries batching functionality. Upon receiving a notification
from Start(), it waits for 1 ms for following burst of notifications. If timeout happens, it
will send a notification to watchdogs for sending AppendEntries.
*/
func StartRateLimiter(rf *Raft, term int) {
	timeout := time.Duration(1) * time.Millisecond
	for {
		rf.mu.Lock()
		// Consistency check
		if rf.killed() || rf.state != Leader || rf.currentTerm != term {
			debug.Debug(debug.DConsist, rf.me, "Watchdog state is inconsistent. isKilled:%v, state:%v, curTerm:%v, watchdogTerm: %v",
				rf.killed(), rf.state, rf.currentTerm, term)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// Wait for initial notification from Start()
		<-rf.startRateLimiter

		t := time.NewTimer(timeout)
		upperT := time.NewTimer(time.Duration(5) * time.Millisecond)
		// Catch burst of notifications from Start()
	inner:
		for {
			select {
			case <-t.C:
				go notifyWatchdogs(rf)
				break inner
			case <-rf.startRateLimiter:
				t.Reset(timeout)
			case <-upperT.C:
				go notifyWatchdogs(rf)
				break inner
			}
		}

	}

}

// This function notifies all watchdogs to send AppendEntries RPC
func notifyWatchdogs(rf *Raft) {
	chanMap := make(map[int]chan int)
	rf.mu.Lock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			chanMap[i] = rf.watchdogChannels[i]
		}
	}
	rf.mu.Unlock()

	// Notifying watchdogs
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			select {
			case chanMap[i] <- 2:
			case <-time.After(5 * time.Millisecond):
				return
			}
		}
	}
	debug.Debug(debug.DInfo, rf.me, "Notified all watchdogs.")
}

func instanceWatchDog(server int, rf *Raft, term int) {
	debug.Debug(debug.DInfo, rf.me, "Starting watchdog for %v.", server)
	// TODO: The correct implementation involves committing a no-op command instead of initial heartbeat.
	go appendEntriesWrapper(rf, server)
	timeout := time.Duration(100) * time.Millisecond
	timer := time.NewTimer(timeout)
	for {
		rf.mu.Lock()
		// Consistency check
		if rf.killed() || rf.state != Leader || rf.currentTerm != term {
			debug.Debug(debug.DConsist, rf.me, "Watchdog state is inconsistent. isKilled:%v, state:%v, curTerm:%v, watchdogTerm: %v",
				rf.killed(), rf.state, rf.currentTerm, term)
			rf.mu.Unlock()
			return
		}
		ch := rf.watchdogChannels[server]
		rf.mu.Unlock()

		select {
		case <-timer.C:
			debug.Debug(debug.DTimer, rf.me, "Watchdog for %v timeout.", server)
			timer.Reset(timeout)
			go appendEntriesWrapper(rf, server)
		case flag := <-ch:
			switch flag {
			case 0:
				// previous AppendEntry RPC was a heartbeat.
				// timer.Reset(timeout)
			case 1:
				// previous AppendEntry RPC was not a heartbeat.
				// timer.Reset(timeout)
				// go appendEntriesWrapper(rf, server)
			case 2:
				// New entry
				timer.Reset(timeout)
				go appendEntriesWrapper(rf, server)
			}

		}
	}
}

func (rf *Raft) main() {
	debug.Debug(debug.DInfo, rf.me, "Starting main.")
	for !rf.killed() {

		// Wait for state transition
		state := <-rf.controlCh

		// Do not block inside this switch statement
		switch state {
		case Candidate:
			debug.Debug(debug.DInfo, rf.me, "Starting an election...")
			go startElection(rf)
		case Leader:
			debug.Debug(debug.DInfo, rf.me, "Starting leader routine.")
			go startLeader(rf)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	debug.Init()

	// initialize from state persisted before a crash
	if persistedData := persister.ReadRaftState(); len(persistedData) < 1 {
		// persistent states
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = Log{}
		rf.log.Initialize()
		rf.snapshot = make([]byte, 0)
		debug.Debug(debug.DPersist, rf.me, "Initializing persistent state.")
	} else {
		rf.readPersist(persistedData)
		rf.snapshot = persister.ReadSnapshot()
		debug.Debug(debug.DPersist, rf.me, "Reading persistent state: curTerm:%v, votedFor:%v, log:%+v.",
			rf.currentTerm, rf.votedFor, rf.log)
	}

	// volatile states
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// custom states
	rf.state = Follower
	rf.lastLeaderPing = time.Now()
	rf.lastInstanceContact = make(map[int]time.Time)
	rf.watchdogChannels = make(map[int]chan int)
	rf.controlCh = make(chan State)
	rf.internalApplyCh = make(chan *LogEntry)
	rf.startRateLimiter = make(chan int)
	rf.applyCh = applyCh

	// start ticker goroutine to start elections
	go rf.ticker()

	// starting main routine
	go rf.main()

	// start applier agent
	go rf.ApplierAgent()

	debug.Debug(debug.DInfo, rf.me, "Starting Raft...")
	return rf
}
