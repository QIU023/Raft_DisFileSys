package main

import (
	"Raft_DisFileSys/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	grpc "google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	*debug = true
	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddrs := []string{}
	if len(args) >= 1 {
		blockStoreAddrs = args
	}

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

func start_meta_server(hostAddr string, meta_server *surfstore.MetaStore) *grpc.Server {
	grpc_server := grpc.NewServer()
	surfstore.RegisterMetaStoreServer(grpc_server, meta_server)
	return grpc_server
}

func start_block_server(hostAddr string, block_server *surfstore.BlockStore) *grpc.Server {
	grpc_server := grpc.NewServer()
	surfstore.RegisterBlockStoreServer(grpc_server, block_server)
	return grpc_server
}

func start_both_server(hostAddr string, meta_server *surfstore.MetaStore, block_server *surfstore.BlockStore) *grpc.Server {
	grpc_server := grpc.NewServer()
	surfstore.RegisterMetaStoreServer(grpc_server, meta_server)
	surfstore.RegisterBlockStoreServer(grpc_server, block_server)
	return grpc_server
}

func Server_serve(hostAddr string, grpc_server *grpc.Server) {
	listener, err := net.Listen("tcp", hostAddr)
	if err != nil {
		log.Fatal(err)
	}

	// log.Printf("Begin Serving gRPC Server")
	if err := grpc_server.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}

func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	// for idx, addr := range blockStoreAddrs {
	// 	blockStoreAddrs[idx] = "blockstore" + addr
	// }
	// log.Print(blockStoreAddrs)
	// consistentHash := surfstore.NewConsistentHashRing(blockStoreAddrs)

	if serviceType == "meta" {
		meta_server := surfstore.NewMetaStore(blockStoreAddrs)
		grpc_server := start_meta_server(hostAddr, meta_server)
		Server_serve(hostAddr, grpc_server)
	} else if serviceType == "block" {
		block_server := surfstore.NewBlockStore()
		grpc_server := start_block_server(hostAddr, block_server)
		Server_serve(hostAddr, grpc_server)
	} else if serviceType == "both" {
		meta_server := surfstore.NewMetaStore(blockStoreAddrs)
		block_server := surfstore.NewBlockStore()
		grpc_server := start_both_server(hostAddr, meta_server, block_server)
		Server_serve(hostAddr, grpc_server)
	} else {
		log.Fatal("unknown service", serviceType)
	}

	return nil
}
