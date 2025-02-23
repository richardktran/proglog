package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	api "github.com/richardktran/proglog/api/v1"
	"github.com/richardktran/proglog/internal/config"
	"github.com/richardktran/proglog/internal/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	localhost = "127.0.0.1"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: localhost,
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: localhost,
	})
	require.NoError(t, err)

	// Create 3 agents
	var agents []*Agent
	for i := 0; i < 3; i++ {
		ports, err := utils.GetFreePort(localhost, 2)
		require.NoError(t, err)

		bindAddr := fmt.Sprintf("%s:%d", localhost, ports[0]) // Port for serf
		rpcPort := ports[1]                                   // Port for gRPC

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i > 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := New(Config{
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			DataDir:         dataDir,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			NodeName:        fmt.Sprintf("agent-%d", i),
			StartJoinAddrs:  startJoinAddrs,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
		})

		require.NoError(t, err)

		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			dataDir := agent.Config.DataDir
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(dataDir))
		}
	}()

	// Wait for agents to setup
	time.Sleep(3 * time.Second)

	leaderClient := setupClient(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)
	require.NoError(t, err)

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, []byte("hello world"), consumeResponse.Record.Value)

	// Wait for replication
	time.Sleep(3 * time.Second)
	followerClient := setupClient(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, []byte("hello world"), consumeResponse.Record.Value)

	// Check out of range offset of leader
	consumeResponse, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset + 1,
		},
	)
	require.Nil(t, consumeResponse)
	require.Error(t, err)
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}

func setupClient(t *testing.T, agent *Agent, peerTLSConfig *tls.Config) api.LogClient {
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(peerTLSConfig)))

	clientConnection, err := grpc.NewClient(rpcAddr, opts...)
	require.NoError(t, err)
	return api.NewLogClient(clientConnection)
}
