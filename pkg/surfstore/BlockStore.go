package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")
	key := blockHash.GetHash()
	if blk, ok := bs.BlockMap[key]; ok {
		return blk, nil
	}
	return nil, fmt.Errorf("%s hash not found", key)
}

func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")
	blockhashes := &BlockHashes{Hashes: make([]string, 0)}
	for k := range bs.BlockMap {
		blockhashes.Hashes = append(blockhashes.Hashes, k)
	}
	return blockhashes, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")
	//
	data := block.GetBlockData()
	succ := &Success{Flag: false}
	if data == nil {
		return succ, fmt.Errorf("Empty data, Get Block Error")
	}
	sha := GetBlockHashString(data)
	bs.BlockMap[sha] = block
	succ.Flag = true
	return succ, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")
	blockHashesOut := &BlockHashes{
		Hashes: []string{},
	}
	for _, hash := range blockHashesIn.GetHashes() {
		if _, ok := bs.BlockMap[hash]; ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, hash)
		}
	}
	return blockHashesOut, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
