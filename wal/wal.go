package wal

import (
	"bufio"
	"encoding/json"
	"github.com/Els-y/kvdb/raft"
	pb "github.com/Els-y/kvdb/rpc"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type WAL struct {
	dir       string
	logFile   string
	stateFile string
	logger    *zap.Logger
}

// TODO: 添加文件锁
//       Restore？名字好像不太对
//       读取恢复 wal
//       转为 pb 减小体积

func Exist(dir string) bool {
	// TODO: 判读文件是否存在，err 其他异常处理？
	_, err := os.Stat(dir)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func Create(dir string, logger *zap.Logger) *WAL {
	err := os.Mkdir(dir, os.ModePerm)
	if err != nil {
		logger.Panic("create wal dir fail", zap.Error(err))
	}

	w := &WAL{
		dir:       dir,
		logFile:   path.Join(dir, "log.json"),
		stateFile: path.Join(dir, "state.json"),
		logger:    logger,
	}
	return w
}

func Restore(dir string, logger *zap.Logger) *WAL {
	// TODO: 函数命名需要修改
	w := &WAL{
		dir:       dir,
		logFile:   path.Join(dir, "log.json"),
		stateFile: path.Join(dir, "state.json"),
		logger:    logger,
	}
	return w
}

func (w *WAL) Save(st pb.HardState, entries []*pb.Entry) {
	if raft.IsEmptyHardState(st) && len(entries) == 0 {
		return
	}

	if err := w.saveEntries(entries); err != nil {
		w.logger.Panic("save entries fail", zap.Error(err))
	}

	if err := w.saveState(st); err != nil {
		w.logger.Panic("save state fail", zap.Error(err))
	}
}

func (w *WAL) saveState(st pb.HardState) error {
	if raft.IsEmptyHardState(st) {
		return nil
	}

	b, err := json.Marshal(st)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(w.stateFile, b, 0666)
	if err != nil {
		w.logger.Error("save state error", zap.Error(err))
		return err
	}

	return nil
}

func (w *WAL) saveEntries(entries []*pb.Entry) error {
	fd, err := os.OpenFile(w.logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	writer := bufio.NewWriter(fd)
	for _, entry := range entries {
		b, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		if n, err := writer.Write(b); n != len(b) || err != nil {
			return err
		}
		if n, err := writer.WriteString("\n"); n != 1 || err != nil {
			return err
		}
	}

	return writer.Flush()
}

func (w *WAL) ReadAll() (pb.HardState, []*pb.Entry, error) {
	state, err := w.readState()
	if err != nil {
		return pb.HardState{}, nil, err
	}

	entries, err := w.readEntries()
	if err != nil {
		return state, nil, err
	}

	return state, entries, nil
}

func (w *WAL) readState() (pb.HardState, error) {
	var st pb.HardState
	b, err := ioutil.ReadFile(w.stateFile)
	if err != nil {
		return st, nil
	}
	err = json.Unmarshal(b, &st)
	w.logger.Info("readState",
		zap.Uint64("term", st.Term),
		zap.Uint64("vote", st.Vote),
		zap.Uint64("commit", st.Commit))
	return st, nil
}

func (w *WAL) readEntries() ([]*pb.Entry, error) {
	var entries []*pb.Entry

	fd, err := os.Open(w.logFile)
	if err != nil {
		return entries, err
	}

	buf := bufio.NewReader(fd)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if len(line) != 0 {
			ent := &pb.Entry{}
			err = json.Unmarshal([]byte(line), &ent)
			w.logger.Info("readEntries",
				zap.Uint64("term", ent.Term),
				zap.Uint64("index", ent.Index))
			entries = append(entries, ent)
		}
		if err == io.EOF {
			return entries, nil
		} else if err != nil {
			return entries, err
		}
	}
}
