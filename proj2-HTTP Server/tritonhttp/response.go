package tritonhttp

import (
	"io"
	"os"
	"sort"
	"strconv"
)

type Response struct {
	Proto      string // e.g. "HTTP/1.1"
	StatusCode int    // e.g. 200
	StatusText string // e.g. "OK"

	// Headers stores all headers to write to the response.
	Headers map[string]string

	// Request is the valid request that leads to this response.
	// It could be nil for responses not resulting from a valid request.
	// Hint: you might need this to handle the "Connection: Close" requirement
	Request *Request

	// FilePath is the local path to the file to serve.
	// It could be "", which means there is no file to serve.
	FilePath string
}

// Write writes the res to the w.
func (res *Response) Write(w io.Writer) error {

	if err := res.WriteStatusLine(w); err != nil {
		return err
	}
	if err := res.WriteSortedHeaders(w); err != nil {
		return err
	}
	if err := res.WriteBody(w); err != nil {
		return err
	}
	return nil
}

// WriteStatusLine writes the status line of res to w, including the ending "\r\n".
// For example, it could write "HTTP/1.1 200 OK\r\n".
func (res *Response) WriteStatusLine(w io.Writer) error {
	switch strconv.Itoa(res.StatusCode) {
	case "200":
		res.StatusText = "OK"
	case "400":
		res.StatusText = "Bad Request"
	case "404":
		res.StatusText = "Not Found"
	}

	statusLine := res.Proto + " " + strconv.Itoa(res.StatusCode) + " " + res.StatusText + "\r\n"

	if _, err := w.Write([]byte(statusLine)); err != nil {
		return err
	}

	return nil
}

// WriteSortedHeaders writes the headers of res to w, including the ending "\r\n".
// For example, it could write "Connection: close\r\nDate: foobar\r\n\r\n".
// For HTTP, there is no need to write headers in any particular order.
// TritonHTTP requires to write in sorted order for the ease of testing.
func (res *Response) WriteSortedHeaders(w io.Writer) error {
	sortedKeys := make([]string, 0, len(res.Headers))

	for key, _ := range res.Headers {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		header := key + ": " + res.Headers[key] + "\r\n"
		if _, err := w.Write([]byte(header)); err != nil {
			return err
		}
	}
	if _, err := w.Write([]byte("\r\n")); err != nil {
		return err
	}

	return nil
}

// WriteBody writes res' file content as the response body to w.
// It doesn't write anything if there is no file to serve.
func (res *Response) WriteBody(w io.Writer) error {
	var content []byte
	var err error
	if res.FilePath != "" {
		if content, err = os.ReadFile(res.FilePath); err != nil {
			return err
		}
	}
	if _, err := w.Write(content); err != nil {
		return err
	}
	return nil
}
