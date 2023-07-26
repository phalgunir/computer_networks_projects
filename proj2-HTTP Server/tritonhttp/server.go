package tritonhttp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"

	// VirtualHosts contains a mapping from host name to the docRoot path
	// (i.e. the path to the directory to serve static files from) for
	// all virtual hosts that this server supports
	VirtualHosts map[string]string
}

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.
func (s *Server) ListenAndServe() error {

	// Create listen socket and spawn off goroutines per incoming client
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	log.Printf("Listening on %q", ln.Addr())

	defer func() {
		err = ln.Close()
		if err != nil {
			log.Printf("Error in closing listener %q", err)
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			//continue?
			return err
		}
		log.Printf("Accepted connection %q", conn.RemoteAddr())
		go s.HandleConnection(conn)
	}
}

// HandleConnection reads requests from the accepted conn and handles them.
func (s *Server) HandleConnection(conn net.Conn) {
	br := bufio.NewReader(conn)

	for {
		// Set a read timeout
		if err := conn.SetReadDeadline(time.Now().Add(RECV_TIMEOUT)); err != nil {
			log.Printf("Failed to set timeout for read %v", conn)
			_ = conn.Close()
			return
		}

		// Read next request from client
		req, bytesReceived, err := ReadRequest(br)

		// // Set a write timeout
		// if err := conn.SetWriteDeadline(time.Now().Add(SEND_TIMEOUT)); err != nil {
		// 	log.Printf("Failed to set timeout for write %v", conn)
		// 	_ = conn.Close()
		// 	return
		// }

		// Handle EOF : Client has closed the connection
		if errors.Is(err, io.EOF) {
			log.Printf("Connection closed by %v", conn.RemoteAddr())
			_ = conn.Close()
			return
		}

		// Handle timeout
		if err, ok := err.(net.Error); ok && err.Timeout() {
			if !bytesReceived {
				log.Printf("Connection to %v timed out", conn.RemoteAddr())
				_ = conn.Close()
				return
			}
			res := &Response{}
			res.HandleBadRequest()
			_ = res.Write(conn)
			time.Sleep(50 * time.Millisecond)
			_ = conn.Close()
			return
		}

		// Handle bad malformed request
		if err != nil {
			log.Printf("Handle bad request for error: %v", err)
			res := &Response{}
			res.HandleBadRequest()
			_ = res.Write(conn)
			time.Sleep(50 * time.Millisecond)
			_ = conn.Close()
			return
		}

		// Handle good request
		log.Printf("Handle good request: %v", req)
		res := s.HandleGoodRequest(req)
		err = res.Write(conn)
		if err != nil {
			fmt.Println(err)
		}

		// Close conn if requested
		if req.Close {
			_ = conn.Close()
			return
		}
	}
}

// HandleGoodRequest handles the valid req and generates the corresponding res.
func (s *Server) HandleGoodRequest(req *Request) (res *Response) {
	res = &Response{}
	res.init(req)
	absPath := filepath.Join(s.VirtualHosts[req.Host], req.URL) //joins and cleans

	if absPath[:len(s.VirtualHosts[req.Host])] != s.VirtualHosts[req.Host] {
		res.HandleNotFound(req)
	} else if _, err := os.Stat(absPath); errors.Is(err, os.ErrNotExist) {
		res.HandleNotFound(req)
	} else {
		res.HandleOK(req, absPath)
	}

	return res
}

// HandleOK prepares res to be a 200 OK response
// ready to be written back to client.
func (res *Response) HandleOK(req *Request, path string) {
	res.StatusCode = 200

	// if path[len(path)-1] == '/'{
	// 	res.FilePath = path + "index.html"
	// } else{
	res.FilePath = path
	// }

	stats, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		log.Print(err)
	}
	res.Headers["Last-Modified"] = FormatTime(stats.ModTime())
	res.Headers["Content-Type"] = MIMETypeByExtension(filepath.Ext(path))
	res.Headers["Content-Length"] = strconv.FormatInt(stats.Size(), 10)
}

// HandleBadRequest prepares res to be a 400 Bad Request response
// ready to be written back to client.
func (res *Response) HandleBadRequest() {
	res.init(nil)
	res.StatusCode = 400
	res.FilePath = ""
	res.Request = nil
	res.Headers["Connection"] = "close"
}

// HandleNotFound prepares res to be a 404 Not Found response
// ready to be written back to client.
func (res *Response) HandleNotFound(req *Request) {
	res.StatusCode = 404
}

func (res *Response) init(req *Request) {
	res.Proto = "HTTP/1.1"
	res.Request = req
	res.Headers = make(map[string]string)
	res.Headers["Date"] = FormatTime(time.Now())
	if req != nil {
		if req.URL[len(req.URL)-1] == '/' {
			req.URL = req.URL + "index.html"
		}
		if req.Close {
			res.Headers["Connection"] = "close"
		}
	}
}
