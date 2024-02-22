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
if Log of other is more up-to-date, returns 1. If two are the same, returns 0.
Otherwise, returns -1.
*/
func CheckLogIsUpToDate(rf *Raft, otherTerm, otherIndex int) int {

	lastLogIndex, lastLogTerm := rf.log.GetLastIndexTerm()

	if otherTerm > lastLogTerm {
		return 1
	} else if otherTerm == lastLogTerm {
		if otherIndex > lastLogIndex {
			return 1
		} else if otherIndex == lastLogIndex {
			return 0
		} else {
			return -1
		}
	} else {
		return -1
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
