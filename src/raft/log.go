src/raft/log.go package raft

type LogStorage struct {
	// first_log_index 1 last_log_index 0
	first_log_index   int
	last_log_index    int
	log               []*LogEntry
}

func (s* LogStorage) getLog() []*LogEntry {
	return s.log
}

func (s* LogStorage) initLogStorage(index int) {
	// Todo 持久化时的初始化方法
	s.first_log_index = index
	s.last_log_index = index - 1
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

func (s* LogStorage) resetLog(index int) {
	s.log = s.log[0 : 0]
	s.first_log_index = index
	s.last_log_index = index - 1
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
	s.log = s.log[:index - s.first_log_index + 1]
	s.last_log_index = index
	if s.first_log_index > s.last_log_index {
		s.first_log_index = s.last_log_index + 1
	}
	return 0
}

func (s* LogStorage) truncatePrefix(index int) {
	s.log = s.log[index - s.first_log_index : ]
	s.first_log_index = index
	if s.last_log_index < index {
		s.last_log_index = s.first_log_index - 1
	}
}

// 支持一次读取多条日志
func (s* LogStorage) getEntry(index, count int) []*LogEntry {
	var entry []*LogEntry
	if s.last_log_index + 1 == s.first_log_index {
		return entry
	}
	if index > s.last_log_index || index < s.first_log_index {
		return entry
	}
	start_index := index - s.first_log_index
	end_index := start_index + count
	if end_index > len(s.log) {
		end_index = len(s.log)
	}
	entry = s.log[start_index : end_index]
	return entry
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
