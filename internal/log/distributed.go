package log

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	api "github.com/richardktran/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var err error
	l.log, err = NewLog(logDir, l.config)

	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log} // Finite State Machine
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logConfig := l.config
	logConfig.Segment.InitialOffset = 1 // Raft requires the initial offset to be 1
	// logStore (where Raft stores those commands)
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// stableStore  is a key-value store where Raft stores important metadata, like the server's current term or the candidate the server voted for.
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	// setup Snapshots, retain = 1 means that we'll keep one snapshot around
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	maxPool := 5                // maxPool is the maximum number of goroutines that the Raft library will use to apply commands to the FSM
	timeout := 10 * time.Second // timeout is the maximum amount of time that the Raft library will wait for a command to be applied

	// transport wraps a stream layer and provides a way for Raft to communicate with other servers in the cluster
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}

	// Election timeout is the amount of time that a follower will wait for a heartbeat from the leader before becoming a candidate
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}

	// LeaderLeaseTimeout is the amount of time that the leader will wait before attempting to renew its leadership
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}

	// CommitTimeout is the time that the leader will wait for a quorum of followers to acknowledge a log entry
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	// HasExistingState checks if there is existing state in the logStore, stableStore, and snapshotStore
	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}

	// BootstrapCluster is used to bootstrap a new cluster
	// hasState is false indicates that this is a new cluster, we need to configure it and bootstrap it
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}

	return err
}

func (l *DistributedLog) Append(record *api.Record) (int64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)

	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Offset, nil
}

func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer

	// Append request type
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	// Marshal and append request to buffer
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	// Apply will send the command to the Raft library, which will replicate it to the other servers in the cluster
	// and apply it to the FSM
	// The future object is a future that will be resolved when the command is applied
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// FSM section
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

// Apply implements raft.FSM.
func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}

	return nil
}

func (f *fsm) applyAppend(buf []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(buf, &req)
	if err != nil {
		return err
	}

	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}

	return &api.ProduceResponse{Offset: offset}
}

// Snapshot implements raft.FSM.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Persist implements raft.FSMSnapshot.
// Persist writes the snapshot to the sink.
// The sink is a stream that the snapshot should be written to.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (s *snapshot) Release() {}

// Restore implements raft.FSM.
func (f *fsm) Restore(snapshot io.ReadCloser) error {
	panic("unimplemented")
}
