package surfstore

import (
	context "context"
	"fmt"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	checkError(err)
	*blockHashes = b.GetHashes()

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		checkError(err)
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		block_store_map, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err == nil {
			if block_store_map == nil {
				return fmt.Errorf("should not return nil block_store_map from leader")
			}
			for k, v := range block_store_map.BlockStoreMap {
				(*blockStoreMap)[k] = v.GetHashes()
			}
			return conn.Close()
		}
		// close the connection
		conn.Close()
	}
	return fmt.Errorf("could not get block_store_map from all servers")
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		*succ = false
		return err
	}

	*succ = b.Flag
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blkhash_obj := &BlockHashes{
		Hashes: blockHashesIn,
	}
	obj_blockHashesOut, err := c.HasBlocks(ctx, blkhash_obj)
	if err != nil {
		conn.Close()
		blockHashesOut = nil
		return err
	}

	*blockHashesOut = obj_blockHashesOut.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// panic("todo")
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		checkError(err)
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		obj_serverFileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err == nil {
			if obj_serverFileInfoMap == nil {
				return fmt.Errorf("should not return nil obj_serverFileInfoMap from leader")
			}
			*serverFileInfoMap = obj_serverFileInfoMap.FileInfoMap
			return conn.Close()
		}
		// close the connection
		conn.Close()
	}
	return fmt.Errorf("could not get obj_serverFileInfoMap from all servers")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// panic("todo")
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		checkError(err)
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		obj_version, err := c.UpdateFile(ctx, fileMetaData)
		if err == nil {
			// update success
			if obj_version == nil {
				*latestVersion = -2
				return fmt.Errorf("should not return nil obj_version from leader")
			}
			*latestVersion = obj_version.Version
			if *latestVersion == -1 {
				log.Printf("Update rejected. Too old Local file! %v", *latestVersion)
			}
			return conn.Close()
		}
		conn.Close()
	}
	// return fmt.Errorf("could not get fileMetaData updated from all servers")
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// panic("todo")
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		checkError(err)
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		obj_blockStoreAddrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err == nil {
			if obj_blockStoreAddrs == nil {
				blockStoreAddrs = nil
				return fmt.Errorf("should not return nil obj_blockStoreAddrs from leader")
			}
			*blockStoreAddrs = obj_blockStoreAddrs.GetBlockStoreAddrs()
			return conn.Close()
		}
		// close the connection
		conn.Close()
	}
	return fmt.Errorf("could not get obj_blockStoreAddrs from all servers")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	// log.Print("creating surfstore RPC client")
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
