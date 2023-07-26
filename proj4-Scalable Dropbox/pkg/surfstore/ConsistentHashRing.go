package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string // serverNameHash: serverName -> swytryerwer: localhost:8080
}

// find where each block belongs to
func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// sort hash values (key in hash ring)
	hashes := []string{}
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)

	// find the first server with larger hash value than blockHash
	// blockHash := c.Hash(blockId) // blockId is a hash
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}
	// fmt.Println("Responsible server : ", responsibleServer, "for Block ID Hash : ", blockId)
	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

// hash servers on a hash ring
func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	var c = ConsistentHashRing{}
	c.ServerMap = make(map[string]string)

	for _, serverAddr := range serverAddrs {
		serverName := "blockstore" + serverAddr
		serverHash := c.Hash(serverName)
		c.ServerMap[serverHash] = serverAddr
	}

	return &c
}
