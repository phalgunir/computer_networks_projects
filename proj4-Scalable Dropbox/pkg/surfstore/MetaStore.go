package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	mtx                sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.Filename
	version := fileMetaData.Version
	m.mtx.Lock()
	if _, ok := m.FileMetaMap[filename]; ok {
		if version == m.FileMetaMap[filename].Version+1 {
			m.FileMetaMap[filename] = fileMetaData
		} else {
			version = -1
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
	}
	m.mtx.Unlock()
	return &Version{Version: version}, nil
}

// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
// }

// Given a list of block hashes, find out which block server they belong to. Returns a mapping from block server address to block hashes
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	str_hash_map := map[string][]string{}
	block_hash_map := map[string]*BlockHashes{}
	// fmt.Println("Inside GetBlockStoreMap...")
	m.mtx.Lock()
	for _, hash := range blockHashesIn.Hashes {
		responsible_server := m.ConsistentHashRing.GetResponsibleServer(hash)
		// fmt.Println(responsible_server)
		str_hash_map[responsible_server] = append(str_hash_map[responsible_server], hash) // "localhost:8080" : ["abc", "xyz"]
	}
	for name, hash_val := range str_hash_map {
		block_hash_map[name] = &BlockHashes{Hashes: hash_val}
	}
	m.mtx.Unlock()

	// fmt.Println(str_hash_map)
	// fmt.Println(block_hash_map)
	return &BlockStoreMap{BlockStoreMap: block_hash_map}, nil
}

// Returns all the BlockStore addresses
func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	var blockStoreAddrs []string
	m.mtx.Lock()
	// for _, address := range m.BlockStoreAddrs {
	// 	blockStoreAddrs = append(blockStoreAddrs, address)
	// }
	blockStoreAddrs = append(blockStoreAddrs, m.BlockStoreAddrs...)
	m.mtx.Unlock()
	return &BlockStoreAddrs{BlockStoreAddrs: blockStoreAddrs}, nil
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
