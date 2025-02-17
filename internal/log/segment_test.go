package log

import (
	"io"
	"os"
	"testing"

	api "github.com/richardktran/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	// Create dir
	dir, err := os.MkdirTemp("", "segment_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Declare record
	record := &api.Record{
		Value: []byte("hello world"),
	}

	// Setup config
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	// Create and test new segment
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	// Test append and read to/from segment
	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(record)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		// Read record
		r, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, record.Value, r.Value)
	}

	// Test max index size
	_, err = s.Append(record)
	require.Equal(t, io.EOF, err) // EOF because index is full
	require.True(t, s.IsMaxed())

	// Adjust config and test max store size
	c.Segment.MaxIndexBytes = 1024
	c.Segment.MaxStoreBytes = uint64(len(record.Value) * 3)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())

	// Remove segment and test if it's maxed
	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
