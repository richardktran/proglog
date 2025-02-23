package log

import (
	"context"
	"log"
	"sync"

	api "github.com/richardktran/proglog/api/v1"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	mu          sync.Mutex
	servers     map[string]chan struct{} // list of servers to replicate to
	closed      bool
	close       chan struct{}
}

// Join adds the given server address to the list of servers
// to replicate and kicks off the add goroutine to run the actual replication logic
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// already added to server and run the replicate logic
		return nil
	}
	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])

	return nil
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if _, ok := r.servers[name]; !ok {
		// server not found
		return nil
	}

	// close the channel to signal the replicate goroutine to stop
	close(r.servers[name])
	// remove the server from the list
	delete(r.servers, name)

	return nil
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}
	r.closed = true

	close(r.close) // signal all replicate goroutines to stop

	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	clientConnection, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		log.Printf("failed to dial %s: %v", addr, err)
		return
	}
	defer clientConnection.Close()

	client := api.NewLogClient(clientConnection)
	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		log.Printf("failed to consume: %v", err)
		return
	}

	records := make(chan *api.Record)

	// Consume records from the stream and send them to the records channel
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				log.Printf("failed to receive: %v", err)
				return
			}

			records <- recv.Record
		}
	}()

	// Produce the records from the records channel to the local server
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err := r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
			if err != nil {
				log.Printf("failed to produce: %v", err)
				return
			}
		}
	}
}

func (r *Replicator) init() {
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}
