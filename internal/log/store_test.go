package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write) + lenWidth)
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test") // store_append_read_test is the prefix of the file name
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(0); i < 3; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, width, n)     // Check if the number of bytes written is equal to the width
		require.Equal(t, width*i, pos) // Check the position of the record in the store
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	pos := uint64(0)
	for i := 0; i < 3; i++ {
		readData, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, readData)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	off := int64(0)
	for i := 0; i < 3; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)

		off += int64(n)

		size := enc.Uint64(b) // Convert 8 bytes into a uint64, which is the length of the record
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b) // Check if the data read is equal to the data written
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)
	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)
	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)

	// The buffer is flushed to the file before closing the file so the size of the file should be greater than before closing the file
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (*os.File, int64, error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
