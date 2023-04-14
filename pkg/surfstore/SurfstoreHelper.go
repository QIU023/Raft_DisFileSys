package surfstore

import (
	context "context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

// const insertTuple string = ``

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		// log.Print(e)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	// panic("todo")

	for filename, fileobj := range fileMetas {
		for hash_index, hash_value := range fileobj.BlockHashList {
			query := "INSERT INTO `indexes` (`fileName`, `version`, `hashindex`, `hashValue`) VALUES (?, ?, ?, ?)"
			insertResult, err := db.ExecContext(context.Background(), query,
				filename, fileobj.Version, hash_index, hash_value)
			if err != nil {
				return err
			}
			_, err = insertResult.LastInsertId()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = ``

const getTuplesByFileName string = ``

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	defer db.Close()
	// panic("todo")
	all_rows, err := db.Query("select * from `indexes`")
	checkError(err)
	defer all_rows.Close()

	for all_rows.Next() {
		var filename, hashvalue string
		var version, hashindex int

		if err := all_rows.Scan(&filename, &version, &hashindex, &hashvalue); err != nil {
			return fileMetaMap, err
		}
		if _, ok := fileMetaMap[filename]; !ok {
			// not seen yet
			fileMetaMap[filename] = &FileMetaData{
				Filename:      filename,
				Version:       (int32)(version),
				BlockHashList: []string{hashvalue},
			}
			// assert they are read in order of hashindex
		} else {
			// seen already, add more hashvalue
			fileMetaMap[filename].BlockHashList = append(fileMetaMap[filename].BlockHashList, hashvalue)
		}
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
