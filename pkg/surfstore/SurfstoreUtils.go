package surfstore

import (
	"bufio"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
)

func compareHashLists(new_fileMetaData, local_fileMetaData *FileMetaData) bool {
	same := true
	if len(new_fileMetaData.BlockHashList) != len(local_fileMetaData.BlockHashList) {
		same = false
	} else {
		for i := 0; i < len(new_fileMetaData.BlockHashList); i++ {
			if new_fileMetaData.BlockHashList[i] != local_fileMetaData.BlockHashList[i] {
				same = false
				break
			}
		}
	}
	return same
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// panic("todo")
	// log.Printf("Client Start Synchonizing")

	block_obj := BlockStoreAddrs{
		BlockStoreAddrs: make([]string, 0),
	}
	err := client.GetBlockStoreAddrs(&block_obj.BlockStoreAddrs)
	checkError(err)
	blockStoreAddrs := &block_obj.BlockStoreAddrs
	// log.Print(blockStoreAddrs)
	// log.Printf("Client Start a")

	localMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	checkError(err)

	sync_completed_file_map := make(map[string]bool, 0)

	files, err := ioutil.ReadDir(client.BaseDir)
	checkError(err)

	all_file_block_map := make(map[string]*map[string][]byte, 0)
	all_file_meta_map := make(map[string]*FileMetaData, 0)
	local_status_map := make(map[string]int, 0)
	// generated from all files exist in dir (except deleted files)

	all_block_size_map := make(map[string]*[]int, 0)
	// 0: local exist, not same with local index.db,
	// ------
	// 1: local exist, same with local index.db,
	//    might split into multiple cases,
	//    because distributed servers out of sync
	// ------
	// 2: local and server both not exist,
	// ------
	// 3: local not exist but server exist,
	// ------
	// 4: local deleted (still exist in index.db, tombstone update),
	//    but first time not yet sync to server
	// ------
	// 5: local deleted, already sync to server, do not add version
	// ------
	// 6: local exist, same with local index.db, split from 1,
	//    but it's different from remote metadata

	// log.Print("initialize")
	for _, file := range files {
		// scan local dir
		if file.IsDir() {
			continue
		}
		filename := file.Name()
		if filename == "index.db" {
			continue
		}
		file_fullpath := ConcatPath(client.BaseDir, filename)
		sync_completed_file_map[filename] = true

		new_fileMetaData := &FileMetaData{
			Filename:      filename,
			Version:       1,
			BlockHashList: make([]string, 0),
		}

		single_blockhash_map := make(map[string][]byte, 0)
		block_size_arr := make([]int, 0)
		getLocalFileBlockHash(client, filename, file_fullpath,
			&single_blockhash_map, &block_size_arr, new_fileMetaData)

		all_block_size_map[filename] = &block_size_arr
		all_file_block_map[filename] = &single_blockhash_map
		all_file_meta_map[filename] = new_fileMetaData

		if local_fileMetaData, ok := localMetaMap[filename]; ok {
			// local index exist curr file, compare hashlist
			same := compareHashLists(new_fileMetaData, local_fileMetaData)
			new_fileMetaData.Version = local_fileMetaData.Version
			if same {
				local_status_map[filename] = 1
			} else {
				local_status_map[filename] = 0
			}
		} else {
			// local index not exist curr file
			local_status_map[filename] = 2
		}
	}

	// remote index.db
	serverFileInfoMap := make(map[string]*FileMetaData, 0)
	err = client.GetFileInfoMap(&serverFileInfoMap)
	checkError(err)

	for filename, local_fileMetaData := range localMetaMap {
		// index.db files, not found locally, marked as deleted
		if _, ok := local_status_map[filename]; !ok {
			new_fileMetaData := &FileMetaData{
				Filename:      filename,
				Version:       local_fileMetaData.Version,
				BlockHashList: []string{"0"},
			}
			all_file_meta_map[filename] = new_fileMetaData
			local_status_map[filename] = 4
		}
	}

	// log.Print("local scan first time finished")

	// log.Print(serverFileInfoMap)

	for filename, server_fileMetaData := range serverFileInfoMap {
		if filename == "index.db" {
			continue
		}
		// remote files

		file_fullpath := ConcatPath(client.BaseDir, filename)
		status, ok := local_status_map[filename]
		if ok {
			// local exist, check version

			local_fileMetaData := all_file_meta_map[filename]
			// log.Print(local_fileMetaData.Version, server_fileMetaData.Version, status)
			if local_fileMetaData.Version < server_fileMetaData.Version {
				// out-of-date, overwrite local version / delete local file
				SyncServerFileToLocal(client, filename,
					file_fullpath, *blockStoreAddrs,
					server_fileMetaData)
				localMetaMap[filename] = server_fileMetaData
			} else if local_fileMetaData.Version == server_fileMetaData.Version {
				// if local_status_map[filename] == 0, same == false, commit new changes
				// log.Print(compareHashLists(server_fileMetaData, local_fileMetaData), status)
				if status == 0 || status == 4 ||
					(status == 1 && !compareHashLists(server_fileMetaData, local_fileMetaData)) {

					new_fileMetaData := all_file_meta_map[filename]
					updated_ver := Version{Version: 1}
					if status == 4 {
						if len(server_fileMetaData.BlockHashList) == 1 && server_fileMetaData.BlockHashList[0] == "0" {
							// log.Printf("Server Already Sync to delete this file")
							continue
						} else {
							new_fileMetaData.Version += 1
						}
					} else {
						new_fileMetaData.Version += 1
					}
					// log.Print(new_fileMetaData.Version)

					err := client.UpdateFile(new_fileMetaData, &updated_ver.Version)
					checkError(err)
					// trigger the commit of new leader, then we need to re-get the new metadata

					// log.Print(updated_ver.Version)
					if updated_ver.Version < 0 {
						// reget new metadata
						err = client.GetFileInfoMap(&serverFileInfoMap)
						checkError(err)

						server_fileMetaData, ok := serverFileInfoMap[filename]
						// log.Print(server_fileMetaData, ok)
						if ok {
							SyncServerFileToLocal(client, filename,
								file_fullpath, *blockStoreAddrs, server_fileMetaData)
							localMetaMap[filename] = server_fileMetaData
						} else {
							// already re-get server metadata, but still empty, bugs or inappropiate multi-thread operation
							log.Fatal("Unexpected server file meta retreve error")
						}
					} else {
						// update success
						// local index.db update
						localMetaMap[filename] = new_fileMetaData
						single_blockhash_map := all_file_block_map[filename]

						if status != 4 {
							// writing new blocks to Block Server, except for deleted files
							UploadLocalBlockToBlockStore(client, filename,
								file_fullpath, *blockStoreAddrs, *all_block_size_map[filename],
								new_fileMetaData, single_blockhash_map)
						}
					}
				}
			} else {
				log.Fatal("Previous Sync Failed! Incorrect Version Kept!")
			}
		} else {
			// local index.db not exist, download, Write (remote exist)
			local_status_map[filename] = 3
			SyncServerFileToLocal(client, filename,
				file_fullpath, *blockStoreAddrs, server_fileMetaData)
			localMetaMap[filename] = server_fileMetaData
		}
	}

	// log.Print("remote scan finished")

	for filename, file_local_status := range local_status_map {
		if filename == "index.db" {
			continue
		}
		// find and update those local new files, they are not exist in local index.db
		if file_local_status == 2 {

			file_fullpath := ConcatPath(client.BaseDir, filename)
			new_fileMetaData := all_file_meta_map[filename]
			updated_ver := Version{Version: 1}
			err := client.UpdateFile(new_fileMetaData, &updated_ver.Version)
			checkError(err)
			// trigger the commit of new leader, then we need to re-get the new metadata

			if updated_ver.Version < 0 {
				// reget new metadata
				err = client.GetFileInfoMap(&serverFileInfoMap)
				checkError(err)
				server_fileMetaData, ok := serverFileInfoMap[filename]
				if ok {
					SyncServerFileToLocal(client, filename,
						file_fullpath, *blockStoreAddrs, server_fileMetaData)
					localMetaMap[filename] = server_fileMetaData
				} else {
					// already re-get server metadata, but still empty, bugs or inappropiate multi-thread operation
					log.Fatal("Unexpected server file meta retreve error")
				}
			} else {
				localMetaMap[filename] = new_fileMetaData
				single_blockhash_map := all_file_block_map[filename]

				// writing new blocks to Block Server
				UploadLocalBlockToBlockStore(client, filename,
					file_fullpath, *blockStoreAddrs, *all_block_size_map[filename],
					new_fileMetaData, single_blockhash_map)
			}
		}
	}

	// log.Print("local new file processed")

	// dump local index.db
	WriteMetaFile(localMetaMap, client.BaseDir)

	// log.Print("local index dumped")

}

func getLocalFileBlockHash(client RPCClient, filename string, file_fullpath string,
	single_blockhash_map *map[string][]byte, block_size_arr *[]int,
	new_fileMetaData *FileMetaData) {

	file_handler, err := os.Open(file_fullpath)
	checkError(err)
	defer file_handler.Close()

	file_info, err := CheckFileExists(file_fullpath)
	checkError(err)
	file_size := int(file_info.Size())
	if file_size == 0 {
		// empty file
		new_fileMetaData.BlockHashList = append(new_fileMetaData.BlockHashList, "-1")
		return
	}

	read_num_blocks := file_size / client.BlockSize
	read_size := []int{}
	for i := 0; i < read_num_blocks; i++ {
		read_size = append(read_size, client.BlockSize)
	}
	remain := file_size % client.BlockSize
	if remain > 0 {
		read_num_blocks += 1
		read_size = append(read_size, remain)
	}
	*block_size_arr = read_size

	reader := bufio.NewReader(file_handler)

	for _, read_size_i := range read_size {
		buffer := make([]byte, client.BlockSize)
		n, err := io.ReadFull(reader, buffer[:read_size_i])

		if err == io.EOF {
			break
		}
		checkError(err)

		hashed_block := GetBlockHashString(buffer[:n])
		new_fileMetaData.BlockHashList = append(new_fileMetaData.BlockHashList, hashed_block)
		(*single_blockhash_map)[hashed_block] = buffer[:n]
	}

}

func CheckFileExists(complete_filePath string) (fs.FileInfo, error) {
	file_info, err := os.Stat(complete_filePath)
	if err != nil {
		if os.IsExist(err) {
			return file_info, nil
		}
		return nil, err
	}
	return file_info, nil
}

func SyncServerFileToLocal(client RPCClient, filename string,
	file_fullpath string, blockStoreAddrs []string,
	server_fileMetaData *FileMetaData) {

	if len(server_fileMetaData.BlockHashList) == 1 && server_fileMetaData.BlockHashList[0] == "0" {
		// deleted file, if file exist
		os.Remove(file_fullpath)
		return
	}

	outfile_handle, err := os.Create(file_fullpath)
	checkError(err)
	defer outfile_handle.Close()

	if len(server_fileMetaData.BlockHashList) == 1 && server_fileMetaData.BlockHashList[0] == "-1" {
		// empty file
		return
	}

	writer := bufio.NewWriter(outfile_handle)

	block_store_map := make(map[string][]string)
	err = client.GetBlockStoreMap(server_fileMetaData.BlockHashList, &block_store_map)
	checkError(err)
	block_store_reverse_map := make(map[string]string)

	for single_blockStoreAddr, block_hash_list := range block_store_map {
		for _, block_hash := range block_hash_list {
			// get reverse addr maps
			block_store_reverse_map[block_hash] = single_blockStoreAddr
		}
	}

	for _, block_hash := range server_fileMetaData.BlockHashList {
		// pull the block
		corr_blockStoreAddr := block_store_reverse_map[block_hash]
		// delete item to release memory ?

		new_block := &Block{BlockData: []byte{}, BlockSize: (int32)(client.BlockSize)}

		err = client.GetBlock(block_hash, corr_blockStoreAddr, new_block)
		checkError(err)

		// dump file
		// log.Print(new_block.BlockData)

		_, err = writer.Write(new_block.BlockData)
		checkError(err)
	}

	writer.Flush()
}

func UploadLocalBlockToBlockStore(client RPCClient, filename string,
	file_fullpath string, blockStoreAddrs []string, block_size_arr []int,
	new_fileMetaData *FileMetaData, single_blockhash_map *map[string][]byte) error {
	// !!! Please check version again before upload,
	// if version isn't latest, then reject and wouldn't enter this func
	// called after the server is updated

	// when there is changes or there is a new files,
	// we need to upload the new blocks

	// distributed version, separate to multiple servers,
	// traverse each server, find non-exist blocks and upload them

	block_store_map := make(map[string][]string)
	err := client.GetBlockStoreMap(new_fileMetaData.BlockHashList, &block_store_map)
	checkError(err)

	// log.Print("start push blocks")

	for single_blockStoreAddr, block_hash_list := range block_store_map {
		// log.Print(single_blockStoreAddr)
		blockHashesOut_i := []string{}
		err := client.HasBlocks(block_hash_list, single_blockStoreAddr, &blockHashesOut_i)
		checkError(err)

		for _, exist_hash := range blockHashesOut_i {
			delete(*single_blockhash_map, exist_hash)
		}

		for _, all_hash := range block_hash_list {
			if data, ok := (*single_blockhash_map)[all_hash]; ok {
				// not exist block in block server, PutBlock for these new blocks
				newBlock := Block{
					BlockData: data,
					// BlockSize: (int32)(block_size_arr[idx]),
					BlockSize: (int32)(client.BlockSize),
				}
				succ := true
				err := client.PutBlock(&newBlock, single_blockStoreAddr, &succ)
				if (!succ) || err != nil {
					return err
				}
			}
		}

	}

	return nil
}
