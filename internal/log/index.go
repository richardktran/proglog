package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth uint64 = offWidth + posWidth // ent is short for entry
)

/*
mmap is a memory-mapped file that stores the index.
memory-mapped file is a segment of virtual memory that has been assigned a direct byte-for-byte correlation with some portion of a file or file-like resource.
*/
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return idx, nil
}

/*
Read receives a relative offset and returns the absolute offset and position of the record in the store.
Index file contains [offset (4 bytes)] [position (8 bytes)]. We read the offset and position from the index file.
*/
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		// read the last entry
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

/*
offset is the offset of the record in the store. (RELATIVE OFFSET)
pos is the position of the record in the store.
*/
func (i *index) Write(off uint32, pos uint64) error {
	if i.size+entWidth > uint64(len(i.mmap)) {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off) // PutUint32 stores the off into the mmap at the position i.size to i.size + offWidth
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)

	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

/*
Problem: When we use newIndex, it will create a new index file, truncate it to the size of the max index bytes, and memory-map the file.
It means that the index file will contain a bunch of empty entries at the end of the file.
When we newIndex again, it will append to the end of the file and has the problem that between the two newIndex calls,
there are several empty entries in the index file.

Solution: We need to truncate the file to the size of the index to remove all the empty entries before create index again.
*/
func (i *index) Close() error {
	// sync the memory-mapped file to disk, MS_SYNC is a flag that tells the kernel to write the data to disk
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// file.Sync() is used to sync the file to disk
	if err := i.file.Sync(); err != nil {
		return err
	}

	// truncate the file to the size of the index
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}
