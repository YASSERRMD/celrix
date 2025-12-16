package celrix

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
)

// Constants
const (
	Magic      = "CELX"
	Version    = 1
	HeaderSize = 22
)

// OpCodes
const (
	OpPing   = 0x01
	OpPong   = 0x02
	OpGet    = 0x03
	OpSet    = 0x04
	OpDel    = 0x05
	OpExists = 0x06

	// Response codes
	OpOk      = 0x10
	OpError   = 0x11
	OpValue   = 0x12
	OpNil     = 0x13
	OpInteger = 0x14
	OpArray   = 0x15

	// Vector ops
	OpVAdd    = 0x20
	OpVSearch = 0x21
)

// Client represents a CELRIX client
type Client struct {
	conn      net.Conn
	rw        *bufio.ReadWriter
	nextReqID uint64
}

// Connect connects to the CELRIX server
func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:      conn,
		rw:        bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		nextReqID: 1,
	}, nil
}

// Close closes the connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// Ping checks server health
func (c *Client) Ping() error {
	if err := c.sendFrame(OpPing, nil); err != nil {
		return err
	}
	resp, err := c.readResponse()
	if err != nil {
		return err
	}
	if s, ok := resp.(string); ok && s == "PONG" {
		return nil
	}
	return fmt.Errorf("unexpected response for PING: %v", resp)
}

// Set sets a key-value pair
func (c *Client) Set(key, value string) error {
	// Payload: [key_len][key][val_len][val][ttl]
	keyBytes := []byte(key)
	valBytes := []byte(value)

	payload := make([]byte, 4+len(keyBytes)+4+len(valBytes)+8)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(keyBytes)))
	offset += 4
	copy(payload[offset:], keyBytes)
	offset += len(keyBytes)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(valBytes)))
	offset += 4
	copy(payload[offset:], valBytes)
	offset += len(valBytes)

	binary.BigEndian.PutUint64(payload[offset:], 0) // TTL 0 = None

	if err := c.sendFrame(OpSet, payload); err != nil {
		return err
	}
	return c.expectOK()
}

// Get gets a value by key
func (c *Client) Get(key string) (string, bool, error) {
	// Payload: [key_len][key]
	keyBytes := []byte(key)
	payload := make([]byte, 4+len(keyBytes))
	binary.BigEndian.PutUint32(payload[0:], uint32(len(keyBytes)))
	copy(payload[4:], keyBytes)

	if err := c.sendFrame(OpGet, payload); err != nil {
		return "", false, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return "", false, err
	}

	if resp == nil {
		return "", false, nil
	}

	if s, ok := resp.(string); ok {
		return s, true, nil
	}

	return "", false, fmt.Errorf("unexpected response type: %T", resp)
}

// Del deletes a key
func (c *Client) Del(key string) (bool, error) {
	keyBytes := []byte(key)
	payload := make([]byte, 4+len(keyBytes))
	binary.BigEndian.PutUint32(payload[0:], uint32(len(keyBytes)))
	copy(payload[4:], keyBytes)

	if err := c.sendFrame(OpDel, payload); err != nil {
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

// VAdd adds a vector
func (c *Client) VAdd(key string, vector []float32) error {
	// Payload: [key_len][key][count][f32...]
	keyBytes := []byte(key)
	payloadLen := 4 + len(keyBytes) + 4 + (len(vector) * 4)
	payload := make([]byte, payloadLen)

	offset := 0
	binary.BigEndian.PutUint32(payload[offset:], uint32(len(keyBytes)))
	offset += 4
	copy(payload[offset:], keyBytes)
	offset += len(keyBytes)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(vector)))
	offset += 4

	for _, f := range vector {
		bits := math.Float32bits(f)
		binary.BigEndian.PutUint32(payload[offset:], bits)
		offset += 4
	}

	if err := c.sendFrame(OpVAdd, payload); err != nil {
		return err
	}
	return c.expectOK()
}

// VSearch searches for similar vectors
func (c *Client) VSearch(vector []float32, k int) ([]string, error) {
	// Payload: [count][f32...][k]
	payloadLen := 4 + (len(vector) * 4) + 4
	payload := make([]byte, payloadLen)

	offset := 0
	binary.BigEndian.PutUint32(payload[offset:], uint32(len(vector)))
	offset += 4

	for _, f := range vector {
		bits := math.Float32bits(f)
		binary.BigEndian.PutUint32(payload[offset:], bits)
		offset += 4
	}

	binary.BigEndian.PutUint32(payload[offset:], uint32(k))

	if err := c.sendFrame(OpVSearch, payload); err != nil {
		return nil, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return nil, err
	}

	if arr, ok := resp.([]interface{}); ok {
		keys := make([]string, len(arr))
		for i, item := range arr {
			if s, ok := item.(string); ok {
				keys[i] = s
			} else {
				keys[i] = fmt.Sprintf("%v", item)
			}
		}
		return keys, nil
	}

	return nil, fmt.Errorf("expected array response, got %T", resp)
}

// Internal helpers

func (c *Client) expectOK() error {
	resp, err := c.readResponse()
	if err != nil {
		return err
	}
	if s, ok := resp.(string); ok && s == "OK" {
		return nil
	}
	return fmt.Errorf("expected OK, got %v", resp)
}

func (c *Client) sendFrame(opcode uint8, payload []byte) error {
	header := make([]byte, HeaderSize)
	copy(header[0:4], []byte(Magic))
	header[4] = uint8(Version)
	header[5] = opcode
	binary.BigEndian.PutUint16(header[6:], 0) // flags
	binary.BigEndian.PutUint32(header[8:], uint32(len(payload)))
	binary.BigEndian.PutUint64(header[12:], c.nextReqID)
	binary.BigEndian.PutUint16(header[20:], 0) // reserved
	c.nextReqID++

	if _, err := c.rw.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := c.rw.Write(payload); err != nil {
			return err
		}
	}
	return c.rw.Flush()
}

func (c *Client) readResponse() (interface{}, error) {
	// Read header
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(c.rw, header); err != nil {
		return nil, err
	}

	magic := string(header[0:4])
	if magic != Magic {
		return nil, fmt.Errorf("invalid magic: %s", magic)
	}

	opcode := header[5]
	payloadLen := binary.BigEndian.Uint32(header[8:])

	// Read payload
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(c.rw, payload); err != nil {
			return nil, err
		}
	}

	switch opcode {
	case OpOk:
		return "OK", nil
	case OpPong:
		return "PONG", nil
	case OpNil:
		return nil, nil
	case OpError:
		return nil, errors.New(string(payload))
	case OpValue:
		return string(payload), nil
	case OpInteger:
		if len(payload) < 8 {
			return nil, errors.New("invalid integer payload")
		}
		return int64(binary.BigEndian.Uint64(payload)), nil
	case OpArray:
		// Basic array parsing for verify: [count: u32][len: u32][bytes]...
		// Implements parsing of simple list of strings/values
		if len(payload) < 4 {
			return []interface{}{}, nil
		}
		count := binary.BigEndian.Uint32(payload[0:])
		offset := 4

		res := make([]interface{}, count)
		for i := 0; i < int(count); i++ {
			if offset+4 > len(payload) {
				return nil, errors.New("incomplete array")
			}
			itemLen := int(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4

			if offset+itemLen > len(payload) {
				return nil, errors.New("incomplete array item")
			}
			res[i] = string(payload[offset : offset+itemLen])
			offset += itemLen
		}
		return res, nil

	default:
		return nil, fmt.Errorf("unknown opcode: %d", opcode)
	}
}
