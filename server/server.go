package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	pb "RicartArgawala/gRPC"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type State int

const (
	RELEASED State = iota
	WANTED
	HELD
)

// queued request struct
type queuedReq struct {
	msg *pb.Message
	ch  chan struct{}
}

// Node struct representing a Ricart–Agrawala node
type Node struct {
	pb.UnimplementedRicartArgawalaServer

	mu sync.Mutex

	id      string
	port    int
	peers   []string
	state   State
	lamport int64

	requestTimestamp int64
	queue            []queuedReq
}

func NewNode(id string, port int, peers []string) *Node {
	return &Node{
		id:      id,
		port:    port,
		peers:   peers,
		state:   RELEASED,
		queue:   []queuedReq{},
		lamport: 0,
	}
}

// Lamport clock update
func (n *Node) bumpOnReceive(t int64) {
	if t > n.lamport {
		n.lamport = t
	}
	n.lamport++
}

// compare (T1, ID1) < (T2, ID2)
func less(t1 int64, id1 string, t2 int64, id2 string) bool {
	if t1 != t2 {
		return t1 < t2
	}
	return id1 < id2
}

// RPC: handle incoming request
func (n *Node) Request(ctx context.Context, m *pb.Message) (*pb.Response, error) {
	n.mu.Lock()
	n.bumpOnReceive(m.Lamport)
	curState := n.state
	myTS := n.requestTimestamp
	myID := n.id
	n.mu.Unlock()

	shouldQueue := false
	if curState == HELD {
		shouldQueue = true
	} else if curState == WANTED {
		if less(myTS, myID, m.Lamport, m.NodeId) {
			shouldQueue = true
		}
	}

	if shouldQueue {
		ch := make(chan struct{})
		q := queuedReq{msg: m, ch: ch}
		n.mu.Lock()
		n.queue = append(n.queue, q)
		n.mu.Unlock()

		select {
		case <-ch:
			return &pb.Response{Msg: "OK"}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &pb.Response{Msg: "OK"}, nil
}

// Internal method to enter CS
func (n *Node) EnterCSInternal() {
	n.mu.Lock()
	n.state = WANTED
	n.lamport++
	n.requestTimestamp = n.lamport
	myTS := n.requestTimestamp
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, addr := range n.peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			connCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := grpc.DialContext(connCtx, peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			if err != nil {
				log.Printf("[%s] Dial %s failed: %v", n.id, peerAddr, err)
				return
			}
			defer conn.Close()
			c := pb.NewRicartArgawalaClient(conn)
			_, err = c.Request(context.Background(), &pb.Message{Lamport: myTS, NodeId: n.id})
			if err != nil {
				log.Printf("[%s] Request to %s failed: %v", n.id, peerAddr, err)
			}
		}(addr)
	}

	wg.Wait()

	n.mu.Lock()
	n.state = HELD
	log.Printf("[%s] Entered HELD (Lamport=%d)", n.id, n.lamport)
	n.mu.Unlock()
}

// Internal method to release CS
func (n *Node) ReleaseCSInternal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = RELEASED
	log.Printf("[%s] Releasing CS (Lamport=%d)", n.id, n.lamport)

	for _, q := range n.queue {
		select {
		case <-q.ch:
		default:
			close(q.ch)
		}
	}
	n.queue = nil
}

func (n *Node) EnterCS(ctx context.Context, e *pb.Empty) (*pb.Response, error) {
	n.EnterCSInternal()
	return &pb.Response{Msg: "Entered CS"}, nil
}

func (n *Node) ReleaseCS(ctx context.Context, e *pb.Empty) (*pb.Response, error) {
	n.ReleaseCSInternal()
	return &pb.Response{Msg: "Released CS"}, nil
}

// Start gRPC server
func (n *Node) RunServer(wg *sync.WaitGroup) {
	defer wg.Done()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", n.port))
	if err != nil {
		log.Fatalf("[%s] failed to listen: %v", n.id, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRicartArgawalaServer(grpcServer, n)
	log.Printf("[%s] Server listening on %d", n.id, n.port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[%s] Server failed: %v", n.id, err)
	}
}

// Automatic CS simulation
func (n *Node) AutoCS() {
	for {
		time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second)
		n.EnterCSInternal()
		hold := time.Duration(rand.Intn(3)+1) * time.Second
		log.Printf("[%s] Holding CS for %v", n.id, hold)
		time.Sleep(hold)
		n.ReleaseCSInternal()
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	nodeID := flag.String("id", "1", "Node ID: 1, 2, or 3")
	flag.Parse()

	ports := map[string]int{
		"1": 50051,
		"2": 50052,
		"3": 50053,
	}

	peerMap := map[string][]string{
		"1": {"localhost:50052", "localhost:50053"},
		"2": {"localhost:50051", "localhost:50053"},
		"3": {"localhost:50051", "localhost:50052"},
	}

	port := ports[*nodeID]
	peers := peerMap[*nodeID]

	node := NewNode("Node"+*nodeID, port, peers)

	var wg sync.WaitGroup
	wg.Add(1)

	go node.RunServer(&wg)

	// Wait a moment for server startup
	time.Sleep(1 * time.Second)

	// Start automatic CS simulation
	go node.AutoCS()

	wg.Wait()
}
