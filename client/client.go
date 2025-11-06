package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "RicartArgawala/gRPC"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	target := flag.String("target", "localhost:50051", "node address to control (host:port)")
	action := flag.String("action", "enter", "action: enter or release")
	hold := flag.Int("hold", 5, "if action==enter: hold CS for this many seconds then auto-release (0 -> no auto release)")
	flag.Parse()

	conn, err := grpc.Dial(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *target, err)
	}
	defer conn.Close()
	c := pb.NewRicartArgawalaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if *action == "enter" {
		response, err := c.EnterCS(ctx, &pb.Empty{})
		if err != nil {
			log.Fatalf("EnterCS failed %v", err)
		}
		fmt.Println("EnterCS response:", response.Msg)
		if *hold > 0 {
			fmt.Printf("Holding CS locally for %d seconds...\n", *hold)
			time.Sleep(time.Duration(*hold) * time.Second)

			ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel2()
			r2, err := c.ReleaseCS(ctx2, &pb.Empty{})
			if err != nil {
				log.Fatalf("ReleaseCS failed: %v", err)
			}
			fmt.Println("ReleaseCS response:", r2.Msg)
		} else {
			fmt.Println("Not auto-releasing; call ReleaseCS separately.")
		}
	} else if *action == "release" {
		response, err := c.ReleaseCS(ctx, &pb.Empty{})
		if err != nil {
			log.Fatalf("ReleaseCS failed: %v", err)
		}
		fmt.Println("ReleaseCS response:", response.Msg)
	} else {
		log.Fatalf("unknown action: %s", *action)
	}
}
