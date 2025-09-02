package main

import (
	"context"
	"fmt"
	"os"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: voyagerctl --endpoints=http://127.0.0.1:2379 <put|get|member|status> ...\n")
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	endpoints := os.Getenv("VOYAGER_ENDPOINTS")
	if endpoints == "" {
		endpoints = "http://127.0.0.1:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoints}})
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	switch os.Args[1] {
	case "put":
		if len(os.Args) != 4 {
			usage()
		}
		_, err := cli.Put(context.Background(), os.Args[2], os.Args[3])
		if err != nil {
			panic(err)
		}
		fmt.Println("OK")
	case "get":
		if len(os.Args) != 3 {
			usage()
		}
		gr, err := cli.Get(context.Background(), os.Args[2])
		if err != nil {
			panic(err)
		}
		for _, kv := range gr.Kvs {
			fmt.Printf("%s=%s\n", kv.Key, kv.Value)
		}
	case "member":
		mr, err := cli.MemberList(context.Background())
		if err != nil {
			panic(err)
		}
		for _, m := range mr.Members {
			fmt.Printf("%d %s %v\n", m.ID, m.Name, m.PeerURLs)
		}
	case "status":
		env, err := cli.Maintenance.Status(context.Background(), endpoints)
		if err != nil {
			panic(err)
		}
		fmt.Printf("version=%s dbSize=%d leader=%d raftTerm=%d\n", env.Version, env.DbSize, env.Leader, env.RaftIndex)
	default:
		usage()
	}
}
