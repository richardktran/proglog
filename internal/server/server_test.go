package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/richardktran/proglog/api/v1"
	"github.com/richardktran/proglog/internal/auth"
	"github.com/richardktran/proglog/internal/config"
	"github.com/richardktran/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, rootClient, nobodyClient api.LogClient, config *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (rootClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Client connection
	newClient := func(certPath, keyPath string) (*grpc.ClientConn, api.LogClient) {
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CAFile:   config.CAFile,
			KeyFile:  keyPath,
			CertFile: certPath,
			Server:   false,
		})
		require.NoError(t, err)
		clientCredentials := credentials.NewTLS(clientTLSConfig)
		clientConnection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(clientCredentials))
		require.NoError(t, err)

		client := api.NewLogClient(clientConnection)
		return clientConnection, client
	}

	var rootClientConnection *grpc.ClientConn
	rootClientConnection, rootClient = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyClientConnection *grpc.ClientConn
	nobodyClientConnection, nobodyClient = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	// Server credentials
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: listener.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCredentials := credentials.NewTLS(serverTLSConfig)

	// Prepare Log
	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	log, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	cfg = &Config{
		CommitLog:  log,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(cfg)
	}

	// Start server
	server, err := NewGRPCServerWithConfig(cfg, grpc.Creds(serverCredentials))
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	teardown = func() {
		server.Stop()
		rootClientConnection.Close()
		nobodyClientConnection.Close()
		listener.Close()
		os.RemoveAll(dir)
	}

	return rootClient, nobodyClient, cfg, teardown
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)

	require.Equal(t, want.Value, consume.Record.Value)
	t.Logf("produce offset: %d, consume offset: %d", produce.Offset, consume.Record.Offset)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	// Test ProduceStream
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, uint64(offset), res.Offset)
		}
	}

	// Test ConsumeStream
	{
		// Get the stream from the server (contains the records)
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		// Loop through the records, get from the stream, and compare with the records
		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})

	if produce != nil {
		t.Fatalf("produce should be nil")
	}

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got err: %v, want: %v", gotCode, wantCode)
	}
}
