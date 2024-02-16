package raft

import (
	"time"

	"6.5840/debug"
)

// Debugging
/* const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
} */

/*
if Log of other is more up-to-date, returns true.
*/
func CheckLogIsUpToDate(rf *Raft, otherTerm, otherIndex int) bool {
	var lastLogIndex int
	var lastLogTerm int
	lastEntry := rf.log.GetLast()
	if lastEntry != nil {
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	} else {
		lastLogIndex = 0
		lastLogTerm = 0
	}

	if otherTerm > lastLogTerm {
		return true
	} else if otherTerm == lastLogTerm {
		if otherIndex >= lastLogIndex {
			return true
		} else {
			return false
		}
	} else {
		return false
	}

}

/*
Perform the operations required when discovering a higher term. (Needs lock)
*/
func DiscoverHigherTerm(rf *Raft, term int) {
	if term != rf.currentTerm {
		debug.Debug(debug.DTerm, rf.me, "Updating term: %v --> %v", rf.currentTerm, term)
		rf.currentTerm = term
	}
	if rf.state != Follower {
		debug.Debug(debug.DState, rf.me, "State change: %v --> %v.", rf.state, Follower)
		rf.state = Follower
	}

	rf.votedFor = -1
	rf.persist()
	// Notify main routine
	rf.controlCh <- Follower
}

// Resets election timer (Lock required)
func ResetElectionTimer(rf *Raft) {
	debug.Debug(debug.DTimer, rf.me, "Resetting timer.")
	rf.lastLeaderPing = time.Now()
}

func SilentResetElectionTimer(rf *Raft) {
	rf.lastLeaderPing = time.Now()
}
