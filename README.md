# A Hash-based Distributed File System with Raft Consensus Algorithm

1. generate the protobuf
```console
protoc --proto_path=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/surfstore/SurfStore.proto
```

2. install the binary executable
```console
make test-install
```

3. using Unit Test to test the code like raft_client_test.go
However, the UT contents are not yet modified to LeaderElection version, still the SetLeader version.
