package raft
// import "log"

type LogStorage struct {
	// first_log_index 1 last_log_index 0
	first_log_index   int
	last_log_index    int
	log               []*LogEntry
}

func (s* LogStorage) initLogStorage() {
	// Todo 持久化时的初始化方法
	s.first_log_index = 1
	s.last_log_index = 0
	s.log = make([]*LogEntry, 0)
}

func (s* LogStorage) lastLogIndex() int {
	return s.last_log_index
}

func (s* LogStorage) firstLogIndex() int {
	return s.first_log_index
}

func (s* LogStorage) lastLogTerm() int {
	if s.first_log_index == s.last_log_index + 1 {
		return 0
	} else {
		return s.log[len(s.log) - 1].Term
	}
}

// AppendEntry to log
// return 0  success
// return -1 fail
func (s* LogStorage) appendEntry(entry *LogEntry) int {
	s.log = append(s.log, entry)
	s.last_log_index++	
	return 0
} 

func (s* LogStorage) appendEntries(entries []*LogEntry) int {
	for idx, entry := range entries {
		if s.appendEntry(entry) != 0 {
			return idx
		}
	}
	return len(entries)
}

func (s* LogStorage) truncateSuffix(index int) int {
	if index > s.last_log_index {
		return -1
	}
	s.log = s.log[:index]
	s.last_log_index = index
	if s.first_log_index > s.last_log_index {
		s.first_log_index = s.last_log_index + 1
	}
	return 0
}

func (s* LogStorage) getEntry(index int) *LogEntry {
	if s.last_log_index + 1 == s.first_log_index {
		return nil
	}
	if index > s.last_log_index || index < s.first_log_index {
		return nil
	}
	return s.log[index - s.first_log_index]
}

func (s* LogStorage) getLogTerm(index int) int {
	if s.last_log_index + 1 == s.first_log_index {
		return 0
	}
	if index > s.last_log_index || index < s.first_log_index {
		return 0
	}
	return s.log[index - s.first_log_index].Term
}


func (s* LogStorage) getLogIndex(index int) int {
	if s.last_log_index + 1 == s.first_log_index {
		return 0
	}
	if index > s.last_log_index || index < s.first_log_index {
		return 0
	}
	return s.log[index - s.first_log_index].LogIndex
}
