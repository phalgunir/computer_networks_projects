package surfstore

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	// read all files in client base directory
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error when reading basedir: ", err)
	}

	// read local index.db into a FileMetaData map
	local_index, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Could not load meta from meta file: ", err)
	}

	// sync local index.db for each file in client base directory
	new_hashmap := make(map[string][]string)
	for _, file := range files {
		// ignore index.db
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		}
		fmt.Println("Syncing local index file", file.Name())

		// compute the number of blocks to divide the file into, based on input blocksize
		var num_blocks int = int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))

		// read the file
		file_to_read, err := os.Open(ConcatPath(client.BaseDir, file.Name()))
		if err != nil {
			log.Println("Error reading file in basedir: ", err)
		}

		// compute hash and append to new_hashmap of the file
		for i := 0; i < num_blocks; i++ {
			byte_slice := make([]byte, client.BlockSize)
			len, err := file_to_read.Read(byte_slice)
			if err != nil {
				log.Println("Error reading bytes from file in basedir: ", err)
			}

			byte_slice = byte_slice[:len]
			hash := GetBlockHashString(byte_slice)
			new_hashmap[file.Name()] = append(new_hashmap[file.Name()], hash)
		}

		// update BlockHashList if hash values have changed and increment the version number
		if val, ok := local_index[file.Name()]; ok {
			if !reflect.DeepEqual(new_hashmap[file.Name()], val.BlockHashList) {
				local_index[file.Name()].BlockHashList = new_hashmap[file.Name()]
				local_index[file.Name()].Version++
			}
		} else {
			// New file
			meta := FileMetaData{
				Filename:      file.Name(),
				Version:       1,
				BlockHashList: new_hashmap[file.Name()]}
			local_index[file.Name()] = &meta
		}
	}

	// set hashvalue to TOMBSTONE_HASHVALUE for deleted files
	for file_name, meta_data := range local_index {
		if _, ok := new_hashmap[file_name]; !ok {
			if len(meta_data.BlockHashList) != 1 || meta_data.BlockHashList[0] != TOMBSTONE_HASHVALUE {
				meta_data.Version++
				meta_data.BlockHashList = []string{TOMBSTONE_HASHVALUE}
			}
		}
	}

	// // get blockstore address
	// var blockStoreAddr string
	// if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
	// 	log.Println("Could not get blockStoreAddr: ", err)
	// }

	// get remote index.db
	remote_index := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remote_index); err != nil {
		log.Println("Error getting index from server: ", err)
		// return
	}

	// check if server has local changes and upload those files
	for file_name, local_meta_data := range local_index {
		// if file already exists in remote
		if remote_meta_data, ok := remote_index[file_name]; ok {
			if local_meta_data.Version > remote_meta_data.Version {
				uploadFile(client, local_meta_data)
			}
		} else {
			// upload the new file created locally
			uploadFile(client, local_meta_data)
		}
	}

	// check for updates on server and download files
	for file_name, remote_meta_data := range remote_index {
		// if file already exists in local
		if local_meta_data, ok := local_index[file_name]; ok {
			// if new version is present in remote
			if local_meta_data.Version < remote_meta_data.Version {
				downloadFile(client, local_meta_data, remote_meta_data)
			} else
			// if same version but blockhashes are different
			if local_meta_data.Version == remote_meta_data.Version && !reflect.DeepEqual(local_meta_data.BlockHashList, remote_meta_data.BlockHashList) {
				downloadFile(client, local_meta_data, remote_meta_data)
			}
		} else {
			// download the new file in remote
			local_index[file_name] = &FileMetaData{}
			localMetaData := local_index[file_name]
			downloadFile(client, localMetaData, remote_meta_data)
		}
	}

	WriteMetaFile(local_index, client.BaseDir)
}

// returns the server address a given block hash belongs to
func getServerForHash(blockStoreMap map[string][]string, input_hash string) string {
	for server, hash_list := range blockStoreMap {
		for _, hash := range hash_list {
			if hash == input_hash {
				return server
			}
		}
	}
	return "None"
}

// uploads local file to remote
func uploadFile(client RPCClient, localMetaData *FileMetaData) error {
	path := ConcatPath(client.BaseDir, localMetaData.Filename)
	var latest_version int32

	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err = client.UpdateFile(localMetaData, &latest_version)
		if err != nil {
			log.Println("Could not upload file: ", err)
		}
		localMetaData.Version = latest_version
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		log.Println("Error opening file: ", err)
	}
	defer file.Close()

	// get a map of "server addr: [block hashes]"
	var blockStoreMap map[string][]string
	if err := client.GetBlockStoreMap(localMetaData.BlockHashList, &blockStoreMap); err != nil {
		log.Println("Could not get blockStoreAddr: ", err)
	}

	file_stat, _ := os.Stat(path)
	var num_blocks int = int(math.Ceil(float64(file_stat.Size()) / float64(client.BlockSize)))

	for i := 0; i < num_blocks; i++ {
		byte_slice := make([]byte, client.BlockSize)
		len, err := file.Read(byte_slice)
		if err != nil && err != io.EOF {
			log.Println("Error reading bytes from file in basedir: ", err)
		}
		byte_slice = byte_slice[:len]

		block := Block{
			BlockData: byte_slice,
			BlockSize: int32(len)}

		// hash the block and identify which server it should be upploaded to
		curr_block_hash := GetBlockHashString(block.BlockData)
		server_to_upload := getServerForHash(blockStoreMap, curr_block_hash)

		if server_to_upload != "None" {
			log.Println("Uploading ", curr_block_hash, "to server ", server_to_upload)
			var succ bool
			if err := client.PutBlock(&block, server_to_upload, &succ); err != nil {
				log.Println("Failed to put block: ", err)
			}
		}
	}

	err = client.UpdateFile(localMetaData, &latest_version)
	if err != nil {
		fmt.Println("Failed to update file: ", err)
		localMetaData.Version = -1
	}
	localMetaData.Version = latest_version

	return nil
}

// downloads remote file to local
func downloadFile(client RPCClient, localMetaData *FileMetaData, remoteMetaData *FileMetaData) error {
	path := ConcatPath(client.BaseDir, remoteMetaData.Filename)

	file, err := os.Create(path)
	if err != nil {
		log.Println("Error creating file: ", err)
	}
	defer file.Close()

	*localMetaData = *remoteMetaData

	// if file is deleted in server
	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == TOMBSTONE_HASHVALUE {
		if err := os.Remove(path); err != nil {
			log.Println("Could not remove local file: ", err)
			return err
		}
		return nil
	}

	// get a map of "server addr: [block hashes]"
	var blockStoreMap map[string][]string
	if err := client.GetBlockStoreMap(remoteMetaData.BlockHashList, &blockStoreMap); err != nil {
		log.Println("Could not get blockStoreAddr: ", err)
	}

	data := ""
	for _, hash := range remoteMetaData.BlockHashList {
		// identify which server to download the block hash from
		server_to_download_from := getServerForHash(blockStoreMap, hash)
		log.Println("Downloading ", hash, "from server ", server_to_download_from)

		var block Block
		if err := client.GetBlock(hash, server_to_download_from, &block); err != nil {
			log.Println("Failed to get block: ", err)
		}
		data += string(block.BlockData)
	}

	file.WriteString(data)
	return nil
}
