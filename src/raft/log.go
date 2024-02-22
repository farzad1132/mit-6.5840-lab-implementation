package raft

import (
	"fmt"
	"slices"

	"6.5840/debug"
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type Log struct {
	Entries           map[int]*LogEntry // Keys are indices
	SortedIndices     []int
	SnapshotLastIndex int
	SnapshotLastTerm  int
	HaveSnapshot      bool
}

func (l *Log) Initialize() {
	l.Entries = make(map[int]*LogEntry)
	l.SortedIndices = make([]int, 0)
	l.SnapshotLastIndex = -1
	l.SnapshotLastTerm = -1
	l.HaveSnapshot = false
}

func (l *Log) InstallSnapshot(lastIndex, LastTerm int) {
	l.SnapshotLastIndex = lastIndex
	l.SnapshotLastTerm = LastTerm
	l.HaveSnapshot = true
}

func (l *Log) Len() int {
	return len(l.SortedIndices)
}

func (l *Log) LastIndex() int {
	len := l.Len()
	if len < 1 {
		if l.HaveSnapshot {
			return l.SnapshotLastIndex
		} else {
			return 0
		}
	} else {
		return l.SortedIndices[len-1]
	}
}

func (l *Log) Add(command interface{}, term, me int) bool {
	newIndex := l.LastIndex() + 1
	entry := LogEntry{
		Term:    term,
		Command: command,
		Index:   newIndex,
	}
	l.Entries[newIndex] = &entry
	l.SortedIndices = append(l.SortedIndices, newIndex)
	slices.Sort(l.SortedIndices)
	debug.Debug(debug.DRep, me, "Added command:%v with term:%v to index:%v", command, term, newIndex)
	return true
}

func (l *Log) Get(index int) (*LogEntry, bool) {
	if e, ok := l.Entries[index]; ok {
		return e, ok
	} else {
		return nil, ok
	}
}

func (l *Log) GetLast() *LogEntry {
	if index := l.LastIndex(); index != -1 {
		e, _ := l.Get(index)
		return e
	} else {
		return nil
	}
}

// This operation is snapshot-aware
func (l *Log) GetLastIndexTerm() (index int, term int) {
	len := l.Len()
	if len < 1 {
		if l.HaveSnapshot {
			return l.SnapshotLastIndex, l.SnapshotLastTerm
		} else {
			return 0, 0
		}
	} else {
		e := l.GetLast()
		return e.Index, e.Term
	}
}

func (l *Log) DeleteFrom(index, me int) {
	debug.Debug(debug.DDrop, me, "Deleting Entries from index:%v.", index)
	origList := make([]int, len(l.SortedIndices))
	if n := copy(origList, l.SortedIndices); n != len(l.SortedIndices) {
		panic("Error: Could not copy all elements of SortedIndices.")
	}
	for realI, i := range origList {
		if i >= index {
			l.SortedIndices = l.SortedIndices[:realI]
			for j := realI; j <= len(origList)-1; j++ {
				delete(l.Entries, origList[j])
			}
			break
		}
	}
	slices.Sort(l.SortedIndices)
}

func (l *Log) DeleteTo(index, me int) {
	debug.Debug(debug.DDrop, me, "Deleting Entries up to index:%v.", index)
	origList := make([]int, len(l.SortedIndices))
	if n := copy(origList, l.SortedIndices); n != len(l.SortedIndices) {
		panic("Error: Could not copy all elements of SortedIndices.")
	}
	for realI := len(origList) - 1; realI >= 0; realI-- {
		i := origList[realI]
		if i <= index {
			l.SortedIndices = l.SortedIndices[realI+1:]
			for j := 0; j <= realI; j++ {
				delete(l.Entries, origList[j])
			}
			break
		}
	}
	slices.Sort(l.SortedIndices)
}

func (l *Log) DeleteAll(me int) {
	debug.Debug(debug.DDrop, me, "Deleting all entries.")
	l.Entries = make(map[int]*LogEntry)
	l.SortedIndices = make([]int, 0)
}

// This operation is snapshot-aware
func (l *Log) FirstIndexOfTerm(term int) int {
	if l.HaveSnapshot && term <= l.SnapshotLastTerm {
		return l.SnapshotLastIndex
	}
	for _, index := range l.SortedIndices {
		if e, ok := l.Get(index); ok {
			if e.Term == term {
				return e.Index
			}
		} else {
			panic(fmt.Sprintf("Error: (FirstIndexOfTerm) Entry with index %v is not in map.", index))
		}
	}
	return -1
}

// This operation is snapshot-aware
func (l *Log) LastIndexOfTerm(term int) int {
	if l.HaveSnapshot && term <= l.SnapshotLastTerm {
		return l.SnapshotLastIndex
	}
	for i := len(l.SortedIndices) - 1; i >= 0; i-- {
		index := l.SortedIndices[i]
		if e, ok := l.Get(index); ok {
			if e.Term == term {
				return e.Index
			}
		} else {
			panic(fmt.Sprintf("Error: (LastIndexOfTerm) Entry with index %v is not in map.", index))
		}
	}
	return -1
}

// This operation is snapshot-aware
func (l *Log) HasTerm(term int) bool {
	for _, e := range l.Entries {
		if e.Term == term {
			return true
		}
	}
	if l.HaveSnapshot && l.SnapshotLastTerm == term {
		return true
	}
	return false
}

func (l *Log) PrintAll() {
	fmt.Printf("logs: ")
	for _, i := range l.SortedIndices {
		if e, ok := l.Get(i); ok {
			fmt.Printf("%+v,", e)
		} else {
			panic(fmt.Sprintf("Error: (PrintAll) Entry with index %v is not in map.", i))
		}
	}
	fmt.Printf("\n")
	fmt.Printf("Sorted indices: %v\n", l.SortedIndices)
	keys := make([]int, 0, len(l.Entries))
	for k := range l.Entries {
		keys = append(keys, k)
	}
	fmt.Printf("Keys: %v\n", keys)
}

func (l *Log) Debug() {
	fmt.Printf("Entries: ")
	for key, e := range l.Entries {
		fmt.Printf("(%v,%+v),", key, e)
	}
	fmt.Printf("\n")
	fmt.Printf("Sorted indices: %v\n", l.SortedIndices)
}
