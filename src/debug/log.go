package debug

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	// levels

	DClient  logTopic = "CLNT"    // Used for any interaction with the **clients**
	DCommit  logTopic = "CMIT"    // Used when **committing or applying a log index**
	DDrop    logTopic = "DROP"    // Used for when **deleting log entries**
	DError   logTopic = "ERRO"    // Used right before panicing
	DInfo    logTopic = "INFO"    // Used for general non-error messages (Also for implementation related logging)
	DLeader  logTopic = "LEAD"    // Used for general **leader** functionalities
	DLog     logTopic = "LOG1"    // Used for nextIndex and matchIndex
	DRep     logTopic = "REPL"    // Used for adding entries to log
	DPersist logTopic = "PERS"    // Used for messages related to **persistence**
	DSnap    logTopic = "SNAP"    // Used for messages related to **snapshots**
	DTerm    logTopic = "TERM"    // Used for **term** related operations
	DRPC     logTopic = "RPC"     // Used for RPC related messages
	DTimer   logTopic = "TIMR"    // Used for **timer** related messages
	DConsist logTopic = "CONSIST" // Used for consistency check messages
	DVote    logTopic = "VOTE"    // Used for **voting** related messages
	DState   logTopic = "STATE"   // Used for **state** related messages
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int
var initialized bool = false

func Init() {
	if !initialized {
		debugVerbosity = getVerbosity()
		debugStart = time.Now()

		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
		initialized = true
	}
}

func Debug(topic logTopic, me int, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Milliseconds()
		time /= 10
		prefix := fmt.Sprintf("%06d | %v | %v | ", time, string(topic), me)
		format = prefix + format + "\n"
		log.Printf(format, a...)
	}
}
