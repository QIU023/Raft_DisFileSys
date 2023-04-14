package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	if m.FileMetaMap != nil {
		ret_map := &FileInfoMap{
			FileInfoMap: m.FileMetaMap,
		}
		return ret_map, nil
	}
	return nil, fmt.Errorf("nil FileInfoMap")
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")
	// log.Print("Updating file in meta store")
	// panic("debug")

	filename := fileMetaData.Filename
	ori_FileMeta, ok := m.FileMetaMap[filename]
	if ok {
		// update a file already exist
		if ori_FileMeta.Version == fileMetaData.Version-1 {
			// update success
			m.FileMetaMap[filename] = fileMetaData
			return &Version{Version: fileMetaData.Version}, nil
			// sycn version from uploaded filemeta
		} else {
			// update failed, too old from client
			return &Version{Version: -1}, nil
		}
	} else {
		// create a new file from client to server
		m.FileMetaMap[filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	}
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	// TODO: call ConsistentHashRing to find the incoming hashes belongs to which server
	// then combining them into a single map,
	// which contains all the hashes for each server
	store_map := &BlockStoreMap{BlockStoreMap: make(map[string]*BlockHashes, 0)}
	for _, hash_val := range blockHashesIn.Hashes {
		responsible_server := m.ConsistentHashRing.GetResponsibleServer(hash_val)
		if store_map.BlockStoreMap[responsible_server] == nil {
			store_map.BlockStoreMap[responsible_server] = &BlockHashes{Hashes: make([]string, 0)}
		}
		store_map.BlockStoreMap[responsible_server].Hashes = append(store_map.BlockStoreMap[responsible_server].Hashes, hash_val)
	}
	// log.Print(store_map.BlockStoreMap)
	return store_map, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
