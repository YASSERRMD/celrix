package celrix

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

// Client represents a CELRIX client
type Client struct {
	conn net.Conn
	rw   *bufio.ReadWriter
}

// Connect connects to the CELRIX server
func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		rw:   bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
	}, nil
}

// Close closes the connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// Set sets a key-value pair
func (c *Client) Set(key, value string) error {
	if err := c.sendCommand("SET", key, value); err != nil {
		return err
	}
	return c.expectOK()
}

// Get gets a value by key
func (c *Client) Get(key string) (string, bool, error) {
	if err := c.sendCommand("GET", key); err != nil {
		return "", false, err
	}
	resp, err := c.readResponse()
	if err != nil {
		return "", false, err
	}
	
	switch v := resp.(type) {
	case string:
		return v, true, nil
	case nil:
		return "", false, nil
	case error:
		return "", false, v
	default:
		return "", false, fmt.Errorf("unexpected response type: %T", v)
	}
}

// Del deletes a key
func (c *Client) Del(key string) (bool, error) {
	if err := c.sendCommand("DEL", key); err != nil {
		return false, err
	}
	resp, err := c.readResponse()
	if err != nil {
		return false, err
	}
	
	if n, ok := resp.(int64); ok {
		return n > 0, nil
	}
	return false, fmt.Errorf("unexpected response type: %T", resp)
}

func (c *Client) sendCommand(args ...string) error {
	// *<num_args>\r\n$<len>\r\n<arg>\r\n...
	c.rw.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		c.rw.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	return c.rw.Flush()
}

func (c *Client) expectOK() error {
	resp, err := c.readResponse()
	if err != nil {
		return err
	}
	if s, ok := resp.(string); ok && s == "OK" {
		return nil
	}
	if err, ok := resp.(error); ok {
		return err
	}
	return fmt.Errorf("expected OK, got %v", resp)
}

func (c *Client) readResponse() (interface{}, error) {
	line, err := c.rw.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 {
		return nil, errors.New("incomplete response")
	}
	
	line = line[:len(line)-2] // Trim \r\n
	if len(line) == 0 {
		return nil, errors.New("empty response")
	}

	switch line[0] {
	case '+': // Simple String
		return line[1:], nil
	case '-': // Error
		return errors.New(line[1:]), nil
	case ':': // Integer
		return strconv.ParseInt(line[1:], 10, 64)
	case '$': // Bulk String
		length, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil // Null
		}
		
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(c.rw, buf); err != nil {
			return nil, err
		}
		return string(buf[:length]), nil
	case '*': // Array
		count, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, nil
		}
		
		res := make([]interface{}, count)
		for i := int64(0); i < count; i++ {
			val, err := c.readResponse()
			if err != nil {
				return nil, err
			}
			res[i] = val
		}
		return res, nil
	default:
		return nil, fmt.Errorf("unknown response type: %c", line[0])
	}
}
