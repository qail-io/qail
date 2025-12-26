package qail

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
)

// Driver provides connection pooling and query execution.
type Driver struct {
	host     string
	port     string
	user     string
	database string
	password string
	sslMode  string
	
	pool     chan *Conn
	poolSize int
	mu       sync.Mutex
}

// Conn represents a single PostgreSQL connection.
type Conn struct {
	conn net.Conn
}

// Config for creating a Driver.
type Config struct {
	Host     string
	Port     string
	User     string
	Database string
	Password string
	PoolSize int
	SSLMode  string // "disable", "require", "prefer"
}

// NewDriver creates a new connection pool.
func NewDriver(cfg Config) (*Driver, error) {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 10
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "prefer"
	}
	
	d := &Driver{
		host:     cfg.Host,
		port:     cfg.Port,
		user:     cfg.User,
		database: cfg.Database,
		password: cfg.Password,
		sslMode:  cfg.SSLMode,
		pool:     make(chan *Conn, cfg.PoolSize),
		poolSize: cfg.PoolSize,
	}
	
	return d, nil
}

// getConn gets a connection from pool or creates new one.
func (d *Driver) getConn() (*Conn, error) {
	select {
	case c := <-d.pool:
		return c, nil
	default:
		return d.connect()
	}
}

// putConn returns connection to pool.
func (d *Driver) putConn(c *Conn) {
	select {
	case d.pool <- c:
	default:
		c.Close()
	}
}

// connect creates a new connection.
func (d *Driver) connect() (*Conn, error) {
	addr := net.JoinHostPort(d.host, d.port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	
	// Try SSL if enabled
	if d.sslMode == "require" || d.sslMode == "prefer" {
		sslConn, err := d.upgradeToSSL(conn)
		if err != nil {
			if d.sslMode == "require" {
				conn.Close()
				return nil, errors.New("SSL required but failed: " + err.Error())
			}
			// prefer mode - continue without SSL
		} else {
			conn = sslConn
		}
	}
	
	c := &Conn{conn: conn}
	
	// Startup handshake
	if err := c.startup(d.user, d.database, d.password); err != nil {
		conn.Close()
		return nil, err
	}
	
	return c, nil
}

// upgradeToSSL attempts SSL/TLS upgrade.
func (d *Driver) upgradeToSSL(conn net.Conn) (net.Conn, error) {
	// Send SSLRequest message
	// Message: 8 bytes - length(8) + SSL code (80877103)
	sslRequest := []byte{0, 0, 0, 8, 4, 210, 22, 47} // len=8, code=80877103
	if _, err := conn.Write(sslRequest); err != nil {
		return nil, err
	}
	
	// Read single byte response
	response := make([]byte, 1)
	if _, err := io.ReadFull(conn, response); err != nil {
		return nil, err
	}
	
	if response[0] != 'S' {
		return nil, errors.New("server does not support SSL")
	}
	
	// Upgrade to TLS
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // For now, skip certificate verification
		ServerName:         d.host,
	}
	
	tlsConn := tls.Client(conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return nil, err
	}
	
	return tlsConn, nil
}

// startup performs PostgreSQL startup handshake.
func (c *Conn) startup(user, database, password string) error {
	// Build startup message (protocol 3.0)
	params := "user\x00" + user + "\x00database\x00" + database + "\x00\x00"
	length := 4 + 4 + len(params)
	
	buf := make([]byte, length)
	binary.BigEndian.PutUint32(buf[0:4], uint32(length))
	binary.BigEndian.PutUint32(buf[4:8], 196608) // Protocol 3.0
	copy(buf[8:], params)
	
	if _, err := c.conn.Write(buf); err != nil {
		return err
	}
	
	// Read response loop
	for {
		msgType, data, err := c.readMessage()
		if err != nil {
			return err
		}
		
		switch msgType {
		case 'R': // Authentication
			authType := binary.BigEndian.Uint32(data[:4])
			switch authType {
			case 0: // AuthenticationOk
				continue
			case 3: // CleartextPassword
				if err := c.sendPassword(password); err != nil {
					return err
				}
			case 5: // MD5Password
				// MD5 auth: md5(md5(password + user) + salt)
				salt := data[4:8]
				if err := c.sendMD5Password(user, password, salt); err != nil {
					return err
				}
			case 10: // SASL (SCRAM-SHA-256)
				return errors.New("SCRAM-SHA-256 not yet implemented - use md5 or trust")
			default:
				return errors.New("unsupported auth method")
			}
		case 'K': // BackendKeyData
			continue
		case 'S': // ParameterStatus
			continue
		case 'Z': // ReadyForQuery
			return nil
		case 'E': // ErrorResponse
			return errors.New("auth error: " + string(data))
		}
	}
}

func (c *Conn) sendPassword(password string) error {
	pwd := password + "\x00"
	length := 4 + len(pwd)
	buf := make([]byte, 1+length)
	buf[0] = 'p'
	binary.BigEndian.PutUint32(buf[1:5], uint32(length))
	copy(buf[5:], pwd)
	_, err := c.conn.Write(buf)
	return err
}

func (c *Conn) sendMD5Password(user, password string, salt []byte) error {
	// MD5 implementation would go here
	// For now, fall back to error
	return errors.New("MD5 password not yet implemented")
}

func (c *Conn) readMessage() (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return 0, nil, err
	}
	
	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:5]) - 4
	
	if length > 0 {
		data := make([]byte, length)
		if _, err := io.ReadFull(c.conn, data); err != nil {
			return 0, nil, err
		}
		return msgType, data, nil
	}
	
	return msgType, nil, nil
}

// FetchAll executes query and returns all rows.
func (d *Driver) FetchAll(cmd *QailCmd) ([]Row, error) {
	c, err := d.getConn()
	if err != nil {
		return nil, err
	}
	defer d.putConn(c)
	
	// Get wire bytes from Rust
	wireBytes := cmd.Encode()
	if wireBytes == nil {
		return nil, errors.New("failed to encode command")
	}
	
	// Send to PostgreSQL
	if _, err := c.conn.Write(wireBytes); err != nil {
		return nil, err
	}
	
	// Read response
	return c.readRows()
}

// Execute executes a command without returning rows.
func (d *Driver) Execute(cmd *QailCmd) error {
	c, err := d.getConn()
	if err != nil {
		return err
	}
	defer d.putConn(c)
	
	wireBytes := cmd.Encode()
	if wireBytes == nil {
		return errors.New("failed to encode command")
	}
	
	if _, err := c.conn.Write(wireBytes); err != nil {
		return err
	}
	
	// Read until ReadyForQuery
	for {
		msgType, data, err := c.readMessage()
		if err != nil {
			return err
		}
		switch msgType {
		case 'Z':
			return nil
		case 'E':
			return errors.New("query error: " + string(data))
		}
	}
}

// BatchExecute executes multiple commands in single round-trip.
func (d *Driver) BatchExecute(cmds []*QailCmd) (int, error) {
	c, err := d.getConn()
	if err != nil {
		return 0, err
	}
	defer d.putConn(c)
	
	// Encode all commands in ONE CGO call
	wireBytes := EncodeBatch(cmds)
	if wireBytes == nil {
		return 0, errors.New("failed to encode batch")
	}
	
	// Send entire batch
	if _, err := c.conn.Write(wireBytes); err != nil {
		return 0, err
	}
	
	// Count completed commands
	completed := 0
	for {
		msgType, data, err := c.readMessage()
		if err != nil {
			return completed, err
		}
		switch msgType {
		case 'C', 'n': // CommandComplete or NoData
			completed++
		case 'Z':
			return completed, nil
		case 'E':
			return completed, errors.New("batch error: " + string(data))
		}
	}
}

// BatchExecuteFast executes batch of SELECT queries with minimal CGO overhead.
// Uses ONE CGO call for the entire batch encoding.
func (d *Driver) BatchExecuteFast(table, columns string, limits []int64) (int, error) {
	c, err := d.getConn()
	if err != nil {
		return 0, err
	}
	defer d.putConn(c)
	
	// ONE CGO call for entire batch!
	wireBytes := EncodeSelectBatchFast(table, columns, limits)
	if wireBytes == nil {
		return 0, errors.New("failed to encode batch")
	}
	
	// Send entire batch
	if _, err := c.conn.Write(wireBytes); err != nil {
		return 0, err
	}
	
	// Count completed commands
	completed := 0
	for {
		msgType, data, err := c.readMessage()
		if err != nil {
			return completed, err
		}
		switch msgType {
		case 'C', 'n': // CommandComplete or NoData
			completed++
		case 'Z':
			return completed, nil
		case 'E':
			return completed, errors.New("batch error: " + string(data))
		}
	}
}

func (c *Conn) readRows() ([]Row, error) {
	var rows []Row
	var colNames []string
	
	for {
		msgType, data, err := c.readMessage()
		if err != nil {
			return nil, err
		}
		
		switch msgType {
		case '1', '2': // ParseComplete, BindComplete
			continue
		case 'T': // RowDescription
			colNames = parseRowDescription(data)
		case 'D': // DataRow
			cols := parseDataRow(data)
			rows = append(rows, Row{columns: cols, names: colNames})
		case 'C': // CommandComplete
			continue
		case 'Z': // ReadyForQuery
			return rows, nil
		case 'E':
			return nil, errors.New("query error: " + string(data))
		}
	}
}

// Close closes all connections.
func (d *Driver) Close() {
	close(d.pool)
	for c := range d.pool {
		c.Close()
	}
}

// Close closes the connection.
func (c *Conn) Close() error {
	// Send Terminate
	c.conn.Write([]byte{'X', 0, 0, 0, 4})
	return c.conn.Close()
}

// Row represents a query result row.
type Row struct {
	columns [][]byte
	names   []string
}

// Get returns column value by index.
func (r Row) Get(idx int) []byte {
	if idx >= 0 && idx < len(r.columns) {
		return r.columns[idx]
	}
	return nil
}

// GetString returns column as string.
func (r Row) GetString(idx int) string {
	b := r.Get(idx)
	if b == nil {
		return ""
	}
	return string(b)
}

// GetInt returns column as int64.
func (r Row) GetInt(idx int) int64 {
	b := r.Get(idx)
	if b == nil {
		return 0
	}
	// Parse text format
	var n int64
	for _, c := range b {
		if c >= '0' && c <= '9' {
			n = n*10 + int64(c-'0')
		}
	}
	return n
}

func parseRowDescription(data []byte) []string {
	colCount := binary.BigEndian.Uint16(data[:2])
	names := make([]string, 0, colCount)
	offset := 2
	
	for i := 0; i < int(colCount); i++ {
		end := offset
		for data[end] != 0 {
			end++
		}
		names = append(names, string(data[offset:end]))
		offset = end + 1 + 18 // Skip null + metadata
	}
	
	return names
}

func parseDataRow(data []byte) [][]byte {
	colCount := binary.BigEndian.Uint16(data[:2])
	cols := make([][]byte, 0, colCount)
	offset := 2
	
	for i := 0; i < int(colCount); i++ {
		length := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		
		if length == -1 {
			cols = append(cols, nil)
		} else {
			cols = append(cols, data[offset:offset+int(length)])
			offset += int(length)
		}
	}
	
	return cols
}
