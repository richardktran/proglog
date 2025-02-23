package discovery

import (
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
)

func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) && // 2 joins
			3 == len(m[0].Members()) && // 3 members
			0 == len(handler.leaves) // 0 leaves
	}, 3*time.Second, 250*time.Millisecond) // wait for 3 seconds, check every 250 milliseconds

	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			3 == len(m[0].Members()) && // 3 members because the member 2 is still in the list
			serf.StatusLeft == m[2].Members()[0].Status && // member 2 left
			1 == len(handler.leaves) // 1 leave
	}, 3*time.Second, 250*time.Millisecond)

	require.Equal(t, fmt.Sprintf("%d", 2), <-handler.leaves)
}

func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	host := "127.0.0.1"
	addr := fmt.Sprintf("%s:%d", host, getFreePort(host))
	tags := map[string]string{
		"rpc_addr": addr,
	}

	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	h := &handler{}
	if len(members) == 0 {
		// first member
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		c.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}
	m, err := New(h, c)
	require.NoError(t, err)
	members = append(members, m)
	return members, h
}

func getFreePort(host string) int {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:0", host))
	if err != nil {
		panic(fmt.Sprintf("can not find any port to assign: %v", err))
	}

	defer func() {
		if err := listener.Close(); err != nil {
			log.Printf("listener.Close(): %v", err)
		}
	}()

	return listener.Addr().(*net.TCPAddr).Port
}

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

// Join pushes the name and address of the joining member to the joins channel.
func (h *handler) Join(name, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"name": name,
			"addr": addr,
		}
	}
	return nil
}

// Leave pushes the name of the leaving member to the leaves channel.
func (h *handler) Leave(name string) error {
	if h.leaves != nil {
		h.leaves <- name
	}

	return nil
}
