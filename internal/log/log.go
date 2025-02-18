package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/richardktran/proglog/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	// Read all existing file from the directory
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64

	// loop through each file and get the base offset from the file name (e.g. 16.store -> 16)

	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name())) // remove the extension
		off, _ := strconv.ParseUint(offStr, 10, 0)                       // 10, 0 means base 10 and 0 bit size
		baseOffsets = append(baseOffsets, off)
	}

	// sort the base offsets of the segments
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// The base offsets contain duplicates because there are two files for each segment (store and index)
	l.removeDuplicates(&baseOffsets)

	// create a new segment for each base offset
	for _, offset := range baseOffsets {
		if err = l.newSegment(offset); err != nil {
			return err
		}
	}

	// if there are no segments, create a new segment with base offset is InitialOffset
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}

	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment

	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || off >= s.nextOffset {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}

	return s.Read(off)
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)

	l.activeSegment = s

	return nil
}

// Close closes all segments
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Remove() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return max(l.segments[len(l.segments)-1].nextOffset-1, 0), nil
}

/*
Truncate removes all segments with a base offset less than the lowest offset we want to keep.
*/
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

func (l *Log) removeDuplicates(offsets *[]uint64) {
	n := len(*offsets)
	i, j := 0, 1
	for j < n {
		if (*offsets)[i] != (*offsets)[j] {
			i++
			(*offsets)[i] = (*offsets)[j]
		}
		j++
	}
}
