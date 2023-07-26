package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func handleRequest(conn net.Conn, ch chan<- []byte) {
	for {
		segmentBuf := make([]byte, 101)
		io.ReadFull(conn, segmentBuf)
		ch <- segmentBuf
		if segmentBuf[0] == 1 {
			break
		}
	}
	conn.Close()
}

func listenOnServer(ch chan<- []byte, host string, port string) {
	l, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Println("Could not listen on the network: ", err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Could not accept the incoming connection: ", err)
		}
		go handleRequest(conn, ch)
	}
}

func sendRecordSet(host string, port string, record_set [][]byte) {
	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			log.Println("Could not dial the host:", host, port, err)
			time.Sleep(50 * time.Millisecond)
		} else {
			break
		}
	}
	defer conn.Close()

	for _, record := range record_set {
		// fmt.Println("Length of record before write ", len(record))
		_, err = conn.Write(record)
		if err != nil {
			log.Println("Could not write the record: ", err)
			// break
		}
	}
	time.Sleep(50 * time.Millisecond)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}
	readPath := os.Args[2]
	writePath := os.Args[3]

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	// initialize serverBits
	serverCount := len(scs.Servers)
	serverBits := int(math.Log2(float64(serverCount)))
	// create a map of {serverId : [record1, record2 ..], where record1 = [1101...]}
	record_set := make(map[int][][]byte)

	// Set up channels and servers to listen and recieve records
	ch := make(chan []byte)
	for _, server := range scs.Servers {
		if serverId == server.ServerId {
			go listenOnServer(ch, server.Host, server.Port)
			fmt.Println("Listening on server : ", server.Host, server.Port)
			break
		}
	}

	// Open input file
	fmt.Println("Reading input file: ", readPath)
	readFile, err := os.Open(readPath)
	if err != nil {
		log.Println("Error opening readfile: ", err)
	}
	reader := bufio.NewReader(readFile)
	if err != nil {
		log.Fatal(err)
	}
	defer readFile.Close()
	time.Sleep(50 * time.Millisecond)

	// Creating Record sets to send other servers
	fmt.Println("Creating Record sets to send other servers")
	for {
		record := make([]byte, 100)
		_, err = io.ReadFull(reader, record)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("Error reading file: ", err)
		}

		// time.Sleep(50 * time.Millisecond)

		// Append records to serverId corresponding to serverBits
		respServerId := int(record[0] >> (8 - serverBits))
		for _, server := range scs.Servers {
			if respServerId == server.ServerId {

				// initialize the slice for key if it doesn't exist yet
				if _, ok := record_set[respServerId]; !ok {
					record_set[respServerId] = make([][]byte, 0)
				}
				record = append([]byte{0}, record...) // add stream_complete
				// time.Sleep(50 * time.Millisecond)
				// fmt.Println(record)

				record_set[respServerId] = append(record_set[respServerId], record)
				break
			}
		}
	}

	time.Sleep(50 * time.Millisecond)

	// Add transfer complete record at the end of record set for all servers
	for _, server := range scs.Servers {
		if serverId != server.ServerId {
			// fmt.Println("Adding transfer complete record at the end of record set for all servers")
			transferCompleted := []byte{}
			for i := 0; i < 100; i++ {
				transferCompleted = append(transferCompleted, 0)
			}
			transferCompleted = append([]byte{1}, transferCompleted...) // stream_complete

			// fmt.Println(server.ServerId, transferCompleted)
			record_set[server.ServerId] = append(record_set[server.ServerId], transferCompleted)
		}
	}

	fmt.Println("Done creating record sets for all servers")
	// fmt.Println(record_set)

	time.Sleep(50 * time.Millisecond)

	// Send records to each server
	for _, server := range scs.Servers {
		if serverId != server.ServerId {
			// fmt.Println(record_set[server.ServerId])
			go sendRecordSet(server.Host, server.Port, record_set[server.ServerId])
			// time.Sleep(50 * time.Millisecond)
		}
	}

	time.Sleep(50 * time.Millisecond)

	// Read own records
	fmt.Println("Reading own records")
	recordArray := [][]byte{}
	totalCompleted := 0
	for {
		// If stream_complete is 1, then processing is complete. Else process record.
		msg := <-ch // [0123123]
		// fmt.Println(msg)
		// fmt.Println(msg[0])
		if msg[0] == 1 {
			totalCompleted += 1
		} else {
			recordArray = append(recordArray, msg[1:])
		}
		// If all servers finished processing
		// fmt.Println("Total completed servers: ", totalCompleted)
		if totalCompleted == serverCount-1 {
			break
		}
	}

	// recordArray = append(recordArray, record_set[serverId]...)
	for _, item := range record_set[serverId] {
		recordArray = append(recordArray, item[1:])
	}
	// for item := range recordArray {
	// 	fmt.Println(len(recordArray[item]))
	// }

	// Sort records
	fmt.Println("Sorting records")
	sort.Slice(recordArray, func(i, j int) bool { return string(recordArray[i][:10]) < string(recordArray[j][:10]) })

	// Write to output file
	fmt.Println("Writing to output file")
	writeFile, err := os.Create(writePath)
	if err != nil {
		log.Println("Error opening writefile: ", err)
	}
	for _, record := range recordArray {
		writeFile.Write(record)
	}
	err = writeFile.Close()
	if err != nil {
		log.Println("Error closing writefile: ", err)
	}
}
