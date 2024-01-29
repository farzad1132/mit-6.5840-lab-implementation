package raft

import "6.5840/debug"

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
func CheckLogIsUpToDate(selfTerm, selfIndex, otherTerm, otherIndex int) bool {
	if otherTerm > selfTerm {
		return true
	} else if otherIndex == selfTerm {
		if otherIndex >= selfIndex {
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
	debug.Debug(debug.DInfo, rf.me, "Fall back to Follower and update term.")
	rf.currentTerm = term
	rf.state = Follower
}
