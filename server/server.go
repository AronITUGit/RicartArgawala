package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sort"
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

type queuedReq struct {
	msg *pb.Message
	ch  chan struct{} //can reply when closed
}

type Server struct {
	pb.UnimplementedRicartArgawalaServer

	mu sync.Mutex

	lamport int64
	id      string

	state State

	requestTimestamp int64

	queue []queuedReq

	peers []string
}

func NewServer(id string, peers []string) *Server {
	return &Server{
		lamport: 0,
		id:      id,
		state:   RELEASED,
		peers:   peers,
		queue:   []queuedReq{},
	}
}

func (s *Server) bumpOnRecieve(t int64) {
	if t > s.lamport {
		s.lamport = t
	}
	s.lamport++
}

func less(t1 int64, id1 string, t2 int64, id2 string) bool {
	if t1 != t2 {
		return t1 < t2
	}
	return id1 < id2
}

func (s *Server) Request(ctx context.Context, m *pb.Message) (*pb.Response, error) {
	s.mu.Lock()
	s.bumpOnRecieve(m.Lamport)
	curState := s.state
	myReqTs := s.requestTimestamp
	myID := s.id
	s.mu.Unlock()

	shouldQueue := false
	if curState == HELD {
		shouldQueue = true
	} else if curState == WANTED {
		if less(myReqTs, myID, m.Lamport, m.NodeId) {
			shouldQueue = true
		}
	}

	if shouldQueue {
		ch := make(chan struct{})
		qr := queuedReq{msg: m, ch: ch}

		s.mu.Lock()
		s.queue = append(s.queue, qr)
		s.mu.Unlock()

		select {
		case <-ch:
			return &pb.Response{Msg: "OK"}, nil
		case <-ctx.Done():
			s.mu.Lock()
			newQ := s.queue[:0]
			for _, q := range s.queue {
				if !(q.msg.NodeId == m.NodeId && q.msg.Lamport == m.Lamport) {
					newQ = append(newQ, q)
				} else {
					select {
					case <-q.ch:
					default:
						close(q.ch)
					}
				}
			}
			s.queue = newQ
			s.mu.Unlock()
			return nil, ctx.Err()
		}
	}
	return &pb.Response{Msg: "OK"}, nil
}

func (s *Server) EnterCS(ctx context.Context, e *pb.Empty) (*pb.Response, error) {
	s.mu.Lock()
	s.state = WANTED
	s.lamport++
	s.requestTimestamp = s.lamport
	mylamport := s.requestTimestamp
	s.mu.Unlock()

	var WG sync.WaitGroup
	replies := make(chan error, len(s.peers))
	for _, p := range s.peers {
		WG.Add(1)
		go func(addr string) {
			defer WG.Done()
			connCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			conn, err := grpc.DialContext(connCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			if err != nil {
				replies <- fmt.Errorf("dial %s: %w", addr, err)
				return
			}
			defer conn.Close()
			c := pb.NewRicartArgawalaClient(conn)
			_, err = c.Request(context.Background(), &pb.Message{Lamport: mylamport, NodeId: s.id})
			if err != nil {
				replies <- fmt.Errorf("request to %s: %w", addr, err)
				return
			}
			replies <- nil
		}(p)
	}

	WG.Wait()
	close(replies)

	for err := range replies {
		if err != nil {
			log.Printf("warning: peer reply error: %v", err)
		}
	}

	s.mu.Lock()
	s.state = HELD
	s.mu.Unlock()

	log.Printf("[%s] Enetered HELD (lamport = %s)", s.id, mylamport)
	return &pb.Response{Msg: "Entered CS"}, nil
}

func (s *Server) ReleaseCS(ctx context.Context, e *pb.Empty) (*pb.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = RELEASED

	for _, q := range s.queue {
		select {
		case <-q.ch:
		default:
			close(q.ch)
		}
	}
	s.queue = nil
	log.Printf("[%s] Released CS and replied to queued requests", s.id)
	return &pb.Response{Msg: "Released CS"}, nil
}

func main() {
	port := flag.Int("port", 50051, "port to listen on")
	id := flag.String("id", "", "node id (unique string)")
	peersCSV := flag.String("peers", "", "comma-separated list of peer gRPC addresses (host:port). Example: localhost:50052,localhost:50053")
	flag.Parse()

	if *id == "" {
		log.Fatalf("must provide id")
	}

	var peers []string
	if *peersCSV != "" {
		for _, p := range splitAndTrim(*peersCSV) {
			if p != fmt.Sprintf("localhost:%d", *port) {
				peers = append(peers, p)
			}
		}
	}

	sort.Strings(peers)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to lsten: %v", err)
	}

	s := grpc.NewServer()
	srv := NewServer(*id, peers)
	pb.RegisterRicartArgawalaServer(s, srv)

	log.Printf("Node %s listening on %d; peers=%v", *id, *port, peers)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to server: %v", err)
	}
}

func splitAndTrim(s string) []string {
	out := []string{}
	start := 0
	cur := ""
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			cur = s[start:i]
			out = append(out, trimSpace(cur))
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, trimSpace(s[start:]))
	}
	return out
}

func trimSpace(s string) string {

	i := 0
	j := len(s) - 1
	for i <= j && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	for j >= i && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
		j--
	}
	if i > j {
		return ""
	}
	return s[i : j+1]
}
