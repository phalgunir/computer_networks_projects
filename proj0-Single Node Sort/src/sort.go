package main

import (
	"io"
	"log"
	"os"
	"sort"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v inputfile outputfile\n", os.Args[0])
	}
	readPath := os.Args[1]
	writePath := os.Args[2]

	// Reading input file
	log.Printf("Reading input file %s\n", readPath)
	readFile, err := os.Open(readPath)
	if err != nil {
		log.Println("Error opening readfile: ", err)
	}

	// Create records in recordArray
	recordArray := [][]byte{}
	for {
		record := make([]byte, 100)     // create a slice of bytes of length 100
		n, err := readFile.Read(record) // read bytes into record
		if err != nil {
			// exit loop at EOF
			if err == io.EOF {
				break
			}
			log.Println("Error reading file: ", err)
		}
		record = record[:n]                       // ensure record has n bytes
		recordArray = append(recordArray, record) // append record to recordArray
	}
	readFile.Close()

	// Sort recordArray based on comparator function that compares first 10bytes of each record
	log.Printf("Sorting %s to %s\n", readPath, writePath)
	sort.Slice(recordArray,
		func(i, j int) bool {
			return string(recordArray[i][:10]) < string(recordArray[j][:10])
		})

	// Writing output file
	log.Printf("Writing output file %s\n", writePath)
	writeFile, err := os.Create(writePath)
	if err != nil {
		log.Println("Error creating writefile: ", err)
	}
	for _, record := range recordArray {
		writeFile.Write(record)
	}
	writeFile.Close()

	log.Println("Done")
}
