package jdb

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/missionMeteora/binny.v2"
)

const (
	entrySet = iota
	entryDelete
)

type Encoder interface {
	Encode(v interface{}) error
}

type Decoder interface {
	Decode(v interface{}) error
}

type Opts struct {
	GetEncoder func(w io.Writer) Encoder
	GetDecoder func(r io.Reader) Decoder
}

var (
	JSON = Opts{
		GetEncoder: func(w io.Writer) Encoder { return json.NewEncoder(w) },
		GetDecoder: func(r io.Reader) Decoder { return json.NewDecoder(r) },
	}

	Gob = Opts{
		GetEncoder: func(w io.Writer) Encoder { return json.NewEncoder(w) },
		GetDecoder: func(r io.Reader) Decoder { return json.NewDecoder(r) },
	}

	Binny = Opts{
		GetEncoder: func(w io.Writer) Encoder { return json.NewEncoder(w) },
		GetDecoder: func(r io.Reader) Decoder { return json.NewDecoder(r) },
	}

	DefaultOpts = JSON
)

type DB struct {
	mux sync.RWMutex
	f   *os.File

	s map[string][]byte

	txPool sync.Pool

	encodeFn func(interface{}) error
	decodeFn func(interface{}) error

	stats struct {
		Rollbacks int64
		Commits   int64
	}
}

func New(fp string, opts *Opts) (*DB, error) {
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &DefaultOpts
	}

	db := &DB{
		f:        f,
		encodeFn: opts.GetEncoder(f).Encode,
		decodeFn: opts.GetDecoder(f).Decode,
		s:        map[string][]byte{},
	}

	db.txPool.New = func() interface{} { return &Tx{db: db, s: storage{}} }

	if err = db.load(); err != nil {
		return nil, err
	}
	_, err = db.f.Seek(0, os.SEEK_END)
	return db, err
}

func (db *DB) load() error {
	db.f.Seek(0, os.SEEK_SET)
	for {
		var tx fileTx
		err := db.decodeFn(&tx)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		// no use for tx.TS for now
		for k, v := range tx.Data {
			switch v.Type {
			case entrySet:
				db.s[k] = v.Value
			case entryDelete:
				delete(db.s, k)
			}
		}
	}
}

func (db *DB) getTx(rw bool) *Tx {
	tx := db.txPool.Get().(*Tx)
	tx.rw = rw
	return tx
}

func (db *DB) putTx(tx *Tx) {
	for k := range tx.s {
		delete(tx.s, k)
	}
	tx.rw = false
	db.txPool.Put(tx)
}

func (db *DB) writeTx(tx *Tx) error {
	// TODO allow non-json
	curPos, err := db.f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	if err := db.encodeFn(&fileTx{time.Now().Unix(), tx.s}); err != nil {
		db.stats.Rollbacks++
		db.f.Seek(curPos, os.SEEK_SET)
		return err
	}
	for k, v := range tx.s {
		switch v.Type {
		case entrySet:
			db.s[k] = v.Value
		case entryDelete:
			delete(db.s, k)
		}
	}
	db.stats.Commits++

	return db.f.Sync()
}

func (db *DB) Read(fn func(tx *Tx) error) error {
	tx := db.getTx(false)
	db.mux.RLock()
	defer func() {
		db.mux.RUnlock()
		db.putTx(tx)
	}()
	return fn(tx)
}

func (db *DB) Update(fn func(tx *Tx) error) error {
	tx := db.getTx(true)
	db.mux.Lock()
	defer func() {
		db.mux.Unlock()
		db.putTx(tx)
	}()
	if err := fn(tx); err != nil {
		db.stats.Rollbacks++
		return err
	}
	return db.writeTx(tx)
}

func (db *DB) Close() error {
	return db.f.Close()
}

func (db *DB) Get(k string) []byte {
	db.mux.RLock()
	v := db.s[k]
	db.mux.RUnlock()
	return v
}

func (db *DB) GetObject(k string, v interface{}) error {
	db.mux.RLock()
	bv := db.s[k]
	db.mux.RUnlock()
	return binny.Unmarshal(bv, v)
}

func (db *DB) Set(k string, v []byte) error {
	return db.Update(func(tx *Tx) error {
		return tx.Set(k, v)
	})
}

func (db *DB) SetObject(k string, v interface{}) error {
	return db.Update(func(tx *Tx) error {
		return tx.SetObject(k, v)
	})
}
