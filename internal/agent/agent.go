package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	api "github.com/richardktran/proglog/api/v1"
	"github.com/richardktran/proglog/internal/auth"
	"github.com/richardktran/proglog/internal/discovery"
	"github.com/richardktran/proglog/internal/log"
	"github.com/richardktran/proglog/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
}

func New(c Config) (*Agent, error) {
	a := &Agent{
		Config:    c,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(
		a.Config.DataDir,
		log.Config{},
	)
	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile)
	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}
	var opts []grpc.ServerOption

	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServerWithConfig(serverConfig, opts...)
	if err != nil {
		return err
	}

	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := a.server.Serve(listener); err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	var opts []grpc.DialOption
	if a.Config.PeerTLSConfig != nil {
		clientCreds := credentials.NewTLS(a.Config.PeerTLSConfig)
		opts = append(opts, grpc.WithTransportCredentials(clientCreds))
	}

	clientConnection, err := grpc.NewClient(rpcAddr, opts...)
	if err != nil {
		return err
	}

	client := api.NewLogClient(clientConnection)

	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}

	a.membership, err = discovery.New(a.replicator, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: rpcAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})

	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			// stop grpc server
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}
