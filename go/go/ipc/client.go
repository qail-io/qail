// Package ipc provides a Unix socket client for communicating with qail-daemon.
// This bypasses CGO overhead by using IPC instead of FFI.
package ipc

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
)

const (
	DefaultSocketPath = "/tmp/qail.sock"
	MaxMessageSize    = 16 * 1024 * 1024 // 16MB
)

// Client is a connection to qail-daemon
type Client struct {
	conn net.Conn
	mu   sync.Mutex
}

// Request types
type Request struct {
	Type    string   `json:"type"`
	DSN     string   `json:"dsn,omitempty"`
	SQL     string   `json:"sql,omitempty"`
	Params  []any    `json:"params,omitempty"`
	Queries []Query  `json:"queries,omitempty"`
}

type Query struct {
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
	Filter  string   `json:"filter,omitempty"`
	Limit   int64    `json:"limit,omitempty"`
}

// Response types
type Response struct {
	Type     string        `json:"type"`
	Rows     []Row         `json:"rows,omitempty"`
	Affected uint64        `json:"affected,omitempty"`
	Results  []QueryResult `json:"results,omitempty"`
	Message  string        `json:"message,omitempty"`
}

type Row struct {
	Columns []any `json:"columns"`
}

type QueryResult struct {
	Rows     []Row  `json:"rows"`
	Affected uint64 `json:"affected"`
}

// Connect creates a new connection to qail-daemon
func Connect(socketPath string) (*Client, error) {
	if socketPath == "" {
		socketPath = DefaultSocketPath
	}

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to qail-daemon: %w", err)
	}

	return &Client{conn: conn}, nil
}

// Close closes the connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Send close request
	req := map[string]any{"Close": struct{}{}}
	c.sendRequest(req)

	return c.conn.Close()
}

// Ping checks if the daemon is alive
func (c *Client) Ping() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{"Ping": struct{}{}}
	resp, err := c.sendRequest(req)
	if err != nil {
		return err
	}

	if resp["type"] != "Pong" {
		return fmt.Errorf("unexpected response: %v", resp)
	}
	return nil
}

// ConnectPG connects to a PostgreSQL database via the daemon
func (c *Client) ConnectPG(host string, port int, user, database, password string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{
		"type":     "Connect",
		"host":     host,
		"port":     port,
		"user":     user,
		"database": database,
		"password": password,
	}
	resp, err := c.sendRequest(req)
	if err != nil {
		return err
	}

	if resp["type"] == "Connected" {
		return nil
	}
	if resp["type"] == "Error" {
		return fmt.Errorf("connection failed: %v", resp["message"])
	}
	return fmt.Errorf("unexpected response: %v", resp)
}

// Get executes a QAIL GET query (SELECT)
func (c *Client) Get(table string, columns []string, limit int64) (*QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{
		"type":    "Get",
		"table":   table,
		"columns": columns,
		"limit":   limit,
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return nil, err
	}

	if resp["type"] == "Results" {
		return parseQueryResult(resp), nil
	}

	if resp["type"] == "Error" {
		return nil, fmt.Errorf("query failed: %v", resp["message"])
	}

	return nil, fmt.Errorf("unexpected response: %v", resp)
}

// Query executes a single query
func (c *Client) Query(sql string, params ...any) (*QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{
		"Query": map[string]any{
			"sql":    sql,
			"params": params,
		},
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return nil, err
	}

	if results, ok := resp["Results"].(map[string]any); ok {
		return parseQueryResult(results), nil
	}

	if errMsg, ok := resp["Error"].(map[string]any); ok {
		return nil, fmt.Errorf("query failed: %s", errMsg["message"])
	}

	return nil, fmt.Errorf("unexpected response: %v", resp)
}

// QueryBatch executes multiple queries in a single IPC call
func (c *Client) QueryBatch(queries []Query) ([]QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{
		"type":    "GetBatch",
		"queries": queries,
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return nil, err
	}

	if resp["type"] == "BatchResults" {
		if results, ok := resp["results"].([]any); ok {
			out := make([]QueryResult, len(results))
			for i, r := range results {
				if m, ok := r.(map[string]any); ok {
					out[i] = *parseQueryResult(m)
				}
			}
			return out, nil
		}
	}

	if resp["type"] == "Error" {
		return nil, fmt.Errorf("batch query failed: %v", resp["message"])
	}

	return nil, fmt.Errorf("unexpected response: %v", resp)
}

// Pipeline executes multiple queries using PostgreSQL pipeline mode (true async)
func (c *Client) Pipeline(queries []Query) ([]QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{
		"type":    "Pipeline",
		"queries": queries,
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return nil, err
	}

	if resp["type"] == "BatchResults" {
		if results, ok := resp["results"].([]any); ok {
			out := make([]QueryResult, len(results))
			for i, r := range results {
				if m, ok := r.(map[string]any); ok {
					out[i] = *parseQueryResult(m)
				}
			}
			return out, nil
		}
	}

	if resp["type"] == "Error" {
		return nil, fmt.Errorf("pipeline failed: %v", resp["message"])
	}

	return nil, fmt.Errorf("unexpected response: %v", resp)
}

// PipelineFast executes multiple queries using PostgreSQL pipeline mode (count only)
// This matches native Rust benchmark performance (no row parsing overhead)
func (c *Client) PipelineFast(queries []Query) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{
		"type":    "PipelineFast",
		"queries": queries,
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return 0, err
	}

	if resp["type"] == "Count" {
		if count, ok := resp["count"].(float64); ok {
			return int(count), nil
		}
	}

	if resp["type"] == "Error" {
		return 0, fmt.Errorf("pipeline fast failed: %v", resp["message"])
	}

	return 0, fmt.Errorf("unexpected response: %v", resp)
}

// Prepare prepares a SQL statement on the server (returns handle for reuse)
// This enables maximum throughput by caching the parsed statement.
func (c *Client) Prepare(sql string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{
		"type": "Prepare",
		"sql":  sql,
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return "", err
	}

	if resp["type"] == "PreparedHandle" {
		if handle, ok := resp["handle"].(string); ok {
			return handle, nil
		}
	}

	if resp["type"] == "Error" {
		return "", fmt.Errorf("prepare failed: %v", resp["message"])
	}

	return "", fmt.Errorf("unexpected response: %v", resp)
}

// PreparedPipeline executes a prepared statement with batched params (FASTEST)
// This matches native Rust performance (~355k q/s)
func (c *Client) PreparedPipeline(handle string, paramsBatch [][]string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := map[string]any{
		"type":         "PreparedPipeline",
		"handle":       handle,
		"params_batch": paramsBatch,
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return 0, err
	}

	if resp["type"] == "Count" {
		if count, ok := resp["count"].(float64); ok {
			return int(count), nil
		}
	}

	if resp["type"] == "Error" {
		return 0, fmt.Errorf("prepared pipeline failed: %v", resp["message"])
	}

	return 0, fmt.Errorf("unexpected response: %v", resp)
}

func (c *Client) sendRequest(req any) (map[string]any, error) {
	// Encode request
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to encode request: %w", err)
	}

	// Write length + data
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := c.conn.Write(lenBuf); err != nil {
		return nil, fmt.Errorf("failed to write length: %w", err)
	}
	if _, err := c.conn.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	// Read response length (must read exactly 4 bytes)
	if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}
	respLen := binary.BigEndian.Uint32(lenBuf)

	if respLen > MaxMessageSize {
		return nil, fmt.Errorf("response too large: %d bytes", respLen)
	}

	// Read response (must read exactly respLen bytes)
	respData := make([]byte, respLen)
	if _, err := io.ReadFull(c.conn, respData); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Decode response
	var resp map[string]any
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return resp, nil
}

func parseQueryResult(m map[string]any) *QueryResult {
	result := &QueryResult{}

	if rows, ok := m["rows"].([]any); ok {
		result.Rows = make([]Row, len(rows))
		for i, r := range rows {
			if rowMap, ok := r.(map[string]any); ok {
				if cols, ok := rowMap["columns"].([]any); ok {
					result.Rows[i] = Row{Columns: cols}
				}
			}
		}
	}

	if affected, ok := m["affected"].(uint64); ok {
		result.Affected = affected
	}

	return result
}
