package jdb

import (
	"compress/gzip"
	"encoding/json"
	"io"
)

type Opts struct {
	Backend func() Backend

	CopyOnSet bool
}

type flusher interface {
	Flush() error
}

// Backend defines a jdb transaction backend
// the backend may optionally implement the io.Closer interface
type Backend interface {
	Init(w io.Writer, r io.Reader) error // initalizes the backend with a reader/writer, usually just an *os.File
	Flush() error                        // this is called after every transaction

	Encode(v interface{}) error // encodes a Tx
	Decode(v interface{}) error // decodes a Tx

	Marshal(in interface{}) ([]byte, error)     // used by SetObject
	Unmarshal(in []byte, out interface{}) error // used by GetObject
}

// DummyBackend returns a DummyBackend where all methods returns ErrNotImpl
func DummyBackend() Backend { return dummyBackend{} }

type dummyBackend struct{}

func (dummyBackend) Init(w io.Writer, r io.Reader) error        { return ErrNotImpl }
func (dummyBackend) Flush() error                               { return ErrNotImpl }
func (dummyBackend) Encode(v interface{}) error                 { return ErrNotImpl }
func (dummyBackend) Decode(v interface{}) error                 { return ErrNotImpl }
func (dummyBackend) Marshal(in interface{}) ([]byte, error)     { return nil, ErrNotImpl }
func (dummyBackend) Unmarshal(in []byte, out interface{}) error { return ErrNotImpl }

func (dummyBackend) New() Backend { return dummyBackend{} }

// JSONBackend returns a json backend.
func JSONBackend() Backend { return &jsonBackend{} }

type jsonBackend struct {
	enc   *json.Encoder
	dec   *json.Decoder
	flush func() error
}

func (j *jsonBackend) Init(w io.Writer, r io.Reader) error {
	j.enc, j.dec = json.NewEncoder(w), json.NewDecoder(r)
	if f, ok := w.(flusher); ok {
		j.flush = f.Flush
	}
	return nil
}

func (j *jsonBackend) Flush() error {
	if j.flush != nil {
		return j.flush()
	}
	return nil
}

func (j *jsonBackend) Encode(v interface{}) error { return j.enc.Encode(v) }
func (j *jsonBackend) Decode(v interface{}) error { return j.dec.Decode(v) }

func (j *jsonBackend) Marshal(in interface{}) ([]byte, error)     { return json.Marshal(in) }
func (j *jsonBackend) Unmarshal(in []byte, out interface{}) error { return json.Unmarshal(in, out) }

// GZipBackend is an alias for  GZipLevelBackend(be, gzip.DefaultCompression)
func GZipBackend(be Backend) Backend { return GZipLevelBackend(be, gzip.DefaultCompression) }

// GZipLevelBackend returns a wrapper backend where all the data is gziped.
func GZipLevelBackend(be Backend, level int) Backend { return &gzipBackend{level: level, be: be} }

type gzipBackend struct {
	level int
	be    Backend
	gzw   *gzip.Writer
}

func (g *gzipBackend) Init(w io.Writer, r io.Reader) error {
	var err error
	if g.gzw, err = gzip.NewWriterLevel(w, g.level); err != nil {
		return err
	}
	gzr := &gzip.Reader{}
	if err := gzr.Reset(r); err != nil && err != io.EOF {
		return err
	}
	return g.be.Init(g.gzw, gzr)
}

func (g *gzipBackend) Flush() error { return g.be.Flush() }

func (g *gzipBackend) Encode(v interface{}) error { return g.be.Encode(v) }
func (g *gzipBackend) Decode(v interface{}) error { return g.be.Decode(v) }

func (g *gzipBackend) Marshal(in interface{}) ([]byte, error)     { return g.be.Marshal(in) }
func (g *gzipBackend) Unmarshal(in []byte, out interface{}) error { return g.be.Unmarshal(in, out) }

func (j *gzipBackend) Close() error {
	return j.gzw.Close()
}

func GZipJSONBackend() Backend { return GZipBackend(JSONBackend()) }

func GZipLevelJSONBackend(level int) Backend { return GZipLevelBackend(JSONBackend(), level) }
