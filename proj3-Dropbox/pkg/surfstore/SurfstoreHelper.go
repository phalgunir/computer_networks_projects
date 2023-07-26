package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

/* SQL related */
const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`
const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`
const getDistinctFileName string = `select distinct fileName from indexes;`
const getTuplesByFileName string = `select version, hashIndex, hashValue from indexes where fileName = ?;`

/*
	Writing Local Metadata File Related
*/

// FileMetaDataToString converts a FileMetaData struct to a string for easy writing back to local metadata file index.db
func FileMetaDataToString(fm *FileMetaData) (result string) {
	result += fm.Filename + CONFIG_DELIMITER
	result += strconv.Itoa(int(fm.Version)) + CONFIG_DELIMITER
	for _, blockHash := range fm.BlockHashList {
		result += blockHash + HASH_DELIMITER
	}
	result += "\n"
	return
}

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {

	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)

	// remove index.db file if it exists
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back : Removing DB file")
		}
	}

	// create new index.db file
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back : Creating new DB file")
	}

	// create table in index.db file with no values
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back : Creating empty table in DB file")
	}
	statement.Exec()

	// prepare to insert values from meta map to index.db
	statement, err = db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During Meta Write Back : Writing values to DB")
	}
	// fmt.Println("Finished creating table")

	for _, fileMeta := range fileMetas {
		result := FileMetaDataToString(fileMeta) // convert map to string for easy addition to DB
		result = result[:len(result)-2]          // remove last "\n" and last HASH_DELIMITER from string
		// fmt.Println("Result : ", result)

		db_column_values := strings.Split(result, CONFIG_DELIMITER)
		db_hash_values := strings.Split(db_column_values[2], HASH_DELIMITER)
		for i := range db_hash_values {
			// fmt.Println(db_column_values[0], db_column_values[1], i, db_hash_values[i])
			statement.Exec(db_column_values[0], db_column_values[1], i, db_hash_values[i]) // insert into DB
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/

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

	// open index.db
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta DB file")
	}

	// scan values : for each distinct file, get all tuples for that file to create map
	var filename string = ""
	var blockHashList []string = nil
	var version int = 0
	var file_hash_index int = 0
	var file_hash_value string = ""

	filenames, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error Reading Distinct FileNames")
	}

	for filenames.Next() {
		filenames.Scan(&filename)
		blockHashList = nil

		file_tuple, err := db.Query(getTuplesByFileName, filename)
		if err != nil {
			log.Fatal("Error Getting Tuples for Filename", filename, err.Error())
		}
		for file_tuple.Next() {
			version = 0
			file_hash_index = 0
			file_hash_value = ""
			file_tuple.Scan(&version, &file_hash_index, &file_hash_value)
			blockHashList = append(blockHashList, file_hash_value)
		}

		// create FileMetaData from scanned values
		currFileMetaData := &FileMetaData{
			Filename:      filename,
			Version:       int32(version),
			BlockHashList: blockHashList,
		}
		// add FileMetaData to map
		fileMetaMap[currFileMetaData.Filename] = currFileMetaData
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
