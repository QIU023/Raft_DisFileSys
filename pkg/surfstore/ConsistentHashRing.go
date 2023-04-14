package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap            map[string]string
	reverseServerMap     map[string]string
	hash_server_location []string
	hash_key_size        int
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// panic("todo")
	// block_hashed_Id := c.Hash(blockId)
	// for _, hashed_blockStoreAddr := range c.hash_server_location {
	// log.Print(c.hash_server_location)

	for i := 0; i < len(c.hash_server_location); i++ {
		hashed_blockStoreAddr := c.hash_server_location[i]

		// log.Print(blockId, "////", hashed_blockStoreAddr)

		if blockId < hashed_blockStoreAddr {
			single_blockStoreAddr := c.reverseServerMap[hashed_blockStoreAddr]
			return single_blockStoreAddr
		}
	}
	return c.reverseServerMap[c.hash_server_location[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// panic("todo")
	// TODO: randomize len(serverAddrs) in 2**k range, then assign them in the map
	k := 8

	con_hash_obj := &ConsistentHashRing{
		ServerMap:            make(map[string]string, 0),
		reverseServerMap:     make(map[string]string, 0),
		hash_server_location: make([]string, 0),
		hash_key_size:        k,
	}
	// log.Print(serverAddrs)
	hash_server_location := make([]string, 0)

	for _, addr := range serverAddrs {
		hashed_addr := con_hash_obj.Hash("blockstore" + addr)
		// 2**k ring mapping

		con_hash_obj.ServerMap[addr] = hashed_addr
		con_hash_obj.reverseServerMap[hashed_addr] = addr
		hash_server_location = append(hash_server_location, hashed_addr)
	}

	sort.Strings(hash_server_location)
	con_hash_obj.hash_server_location = hash_server_location
	// log.Print(con_hash_obj.ServerMap, con_hash_obj.reverseServerMap)

	return con_hash_obj
}
