package utils

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreePort(t *testing.T) {
	ports, err := GetFreePort("127.0.0.1", 2)
	require.NoError(t, err)

	require.Equal(t, 2, len(ports))
	require.NotEqual(t, ports[0], ports[1])

	// Check that the ports are released and can be reused
	for _, port := range ports {
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err, "Port %d should be free for reuse", port)
		require.NotNil(t, listener)
		_ = listener.Close()
	}
}
