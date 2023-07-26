package surfstore

import (
	context "context"
	"errors"
	"fmt"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
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
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		return err
	}
	*succ = b.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// fmt.Println("Inside Get File Info Map RPC call...")
	for _, metaStore := range surfClient.MetaStoreAddrs {
		// fmt.Println(metaStore)
		conn, err := grpc.Dial(metaStore, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// fmt.Println(ctx)
		f, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		if err != nil {
			fmt.Println("Encountered error..", err)
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		*serverFileInfoMap = f.FileInfoMap
		return nil
	}
	return errors.New("Cluster is down")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// fmt.Println("Inside Update File RPC")
	for _, metaStore := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStore, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// fmt.Println("Calling update file mdt")
		v, err := c.UpdateFile(ctx, fileMetaData)

		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				// fmt.Println("Server crashed.. Continuing without closing connection and returning error. ")
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				// fmt.Println("Server not leader.. Continuing without closing connection and returning error. ")
				continue
			}
			fmt.Println("Got error and closed connection", err)
			conn.Close()
			return err
		}
		*latestVersion = v.Version
		// fmt.Println("Allocated latest version as ", v.Version)
		return conn.Close()
	}

	// fmt.Println("Returning cluser down")
	return errors.New("Cluster is down")
}

// func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
// 	// connect to the server
// 	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		return err
// 	}
// 	c := NewMetaStoreClient(conn)

// 	// perform the call
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()
// 	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
// 	if err != nil {
// 		conn.Close()
// 		return err
// 	}
// 	*blockStoreAddr = addr.Addr

// 	// close the connection
// 	return conn.Close()
// }

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Println("Error in RPC GetBlockHashes", err.Error())
		conn.Close()
		return err
	}
	*blockHashes = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, metaStore := range surfClient.MetaStoreAddrs {
		// fmt.Println("Inside Get Block Store map RPC")
		conn, err := grpc.Dial(metaStore, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		bm, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})

		if err != nil {
			fmt.Println("Encountered error", err)
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if !strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}

		str_hash_map := map[string][]string{}
		// fmt.Println(bm.BlockStoreMap)
		for server, server_hash := range bm.BlockStoreMap {
			str_hash_map[server] = append(str_hash_map[server], server_hash.Hashes...)
		}
		*blockStoreMap = str_hash_map
		return conn.Close()
	}
	return errors.New("Cluster is down")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, metaStore := range surfClient.MetaStoreAddrs {
		// fmt.Println("Inside Get Block Store Addrs RPC")
		conn, err := grpc.Dial(metaStore, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		addr, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})

		if err != nil {
			fmt.Println("Encountered error", err)
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if !strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		*blockStoreAddrs = addr.BlockStoreAddrs
		return conn.Close()
	}
	return errors.New("Cluster is down")
}

// This line guarantees all methods for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create a Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
