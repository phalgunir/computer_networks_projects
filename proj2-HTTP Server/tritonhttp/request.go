package tritonhttp

import (
	"bufio"
	"fmt"
	"strings"
)

type Request struct {
	Method string // e.g. "GET"
	URL    string // e.g. "/path/to/a/file"
	Proto  string // e.g. "HTTP/1.1"

	// Headers stores the key-value HTTP headers
	Headers map[string]string

	Host  string // determine from the "Host" header
	Close bool   // determine from the "Connection" header
}

func ReadRequest(br *bufio.Reader) (req *Request, bytesReceived bool, err error) {
	// Error handling
	req = &Request{}
	req.Headers = make(map[string]string)

	// Read start line
	line, err := ReadLine(br)
	if err != nil {
		fmt.Println("Error in ReadLine", err)
		return nil, true, fmt.Errorf("400")
	}

	req.Method, req.URL, req.Proto, err = parseRequestLine(line)
	if err != nil {
		return nil, true, err
	}
	if req.Method != "GET" || req.URL[0] != '/' || req.Proto != "HTTP/1.1" {
		return nil, true, fmt.Errorf("400")
	}

	// Read headers
	hasHost := false
	req.Close = false
	for {
		line, err := ReadLine(br)
		// fmt.Printf(line)
		if err != nil {
			fmt.Println("Error in ReadLine", err)
			return nil, true, err
		}
		if line == "" {
			break // This marks header end : !!!! throw into map and return map
		}
		// Check required headers
		fields := strings.SplitN(line, ":", 2)
		if len(fields) != 2 {
			return nil, true, fmt.Errorf("400")
		}
		key := CanonicalHeaderKey(strings.TrimSpace(fields[0]))
		value := strings.TrimSpace(fields[1])

		// Handle special headers
		if key == "Host" {
			req.Host = value
			hasHost = true
		} else if key == "Connection" {
			if value == "close" {
				req.Close = true
			} else {
				return nil, true, fmt.Errorf("400")
			}
		} else {
			req.Headers[key] = value
		}
	}

	if !hasHost {
		return nil, true, fmt.Errorf("400")
	}

	return req, true, nil
}

func parseRequestLine(line string) (Method string, URL string, Proto string, err error) {
	fields := strings.SplitN(line, " ", 3)
	if len(fields) != 3 {
		return "", "", "", fmt.Errorf("could not parse the request line, got fields %v", fields)
	}
	return fields[0], fields[1], fields[2], nil
}

// Read line by line
func ReadLine(br *bufio.Reader) (string, error) {
	// delim := flag.String("delimiter", "\r\n", "CRLF Delimiter used to separate lines")
	// flag.Parse()
	// var line string

	// remaining := ""
	// buf := make([]byte, 10)

	// for {
	// 	for strings.Contains(remaining, *delim) {
	// 		idx := strings.Index(remaining, *delim)
	// 		line += remaining[:idx]
	// 		remaining = remaining[idx+1:]
	// 	}

	// 	size, err := br.Read(buf)
	// 	if err != nil {
	// 		return line, err
	// 	}
	// 	data := buf[:size]
	// 	remaining = remaining + string(data)
	// }
	// return line, nil

	var line string

	for {
		s, err := br.ReadString('\n')
		line += s
		if err != nil {
			return line, err
		}
		if strings.HasSuffix(line, "\r\n") {
			line = line[:len(line)-2]
			return line, nil
		} else {
			return line, err
		}
	}
}
