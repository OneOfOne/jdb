package jdb

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/missionMeteora/binny.v2"
)

const (
	entrySet = iota
	entryDelete
	entryCompact
)

type Encoder interface {
	Encode(v interface{}) error
}

type Decoder interface {
	Decode(v interface{}) error
}

type Opts struct {
	GetEncoder  func(w io.Writer) Encoder
	GetDecoder  func(r io.Reader) Decoder
	Marshaler   func(in interface{}) ([]byte, error)
	Unmarshaler func(in []byte, out interface{}) error

	CopyOnSet bool
	CopyOnGet bool
}

var (
	JSON = Opts{
		GetEncoder:  func(w io.Writer) Encoder { return json.NewEncoder(w) },
		GetDecoder:  func(r io.Reader) Decoder { return json.NewDecoder(r) },
		Marshaler:   json.Marshal,
		Unmarshaler: json.Unmarshal,
	}

	// Gob = Opts{
	// 	GetEncoder:  func(w io.Writer) Encoder { return gob.NewEncoder(w) },
	// 	GetDecoder:  func(r io.Reader) Decoder { return gob.NewDecoder(r) },
	// }

	Binny = Opts{
		GetEncoder:  func(w io.Writer) Encoder { return binny.NewEncoder(w) },
		GetDecoder:  func(r io.Reader) Decoder { return binny.NewDecoder(r) },
		Marshaler:   binny.Marshal,
		Unmarshaler: binny.Unmarshal,
	}

	defaultOpts = JSON
)

type Bucket map[string]Value

type DB struct {
	mux sync.RWMutex
	f   *os.File

	s map[string]Bucket

	maxIndex uint64

	txPool sync.Pool

	encodeFn func(interface{}) error
	decodeFn func(interface{}) error

	opts Opts

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
		opts = &defaultOpts
	}

	if opts.GetEncoder == nil || opts.GetDecoder == nil {
		opts.GetEncoder = defaultOpts.GetEncoder
		opts.GetDecoder = defaultOpts.GetDecoder
		opts.Marshaler = defaultOpts.Marshaler
		opts.Unmarshaler = defaultOpts.Unmarshaler
	}

	db := &DB{
		s:    map[string]Bucket{},
		opts: *opts,
	}
	db.init(f)
	db.txPool.New = func() interface{} { return &Tx{db: db, s: storage{}} }

	if err = db.load(); err != nil {
		return nil, err
	}
	db.maxIndex++
	_, err = db.f.Seek(0, os.SEEK_END)
	return db, err
}

func (db *DB) init(f *os.File) {
	db.f = f
	db.encodeFn = db.opts.GetEncoder(f).Encode
	db.decodeFn = db.opts.GetDecoder(f).Decode
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

		tx.Data.apply(db)
		for k, v := range tx.CompactData {
			db.s[k] = v
		}
		db.maxIndex = tx.Index
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
	curPos, err := db.f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	if err := db.encodeFn(&fileTx{
		Index: db.maxIndex,
		TS:    time.Now().Unix(),
		Data:  tx.s,
	}); err != nil {
		db.stats.Rollbacks++
		db.f.Truncate(curPos)
		return err
	}

	if err := db.f.Sync(); err != nil {
		db.stats.Rollbacks++
		db.f.Truncate(curPos)
		return err
	}

	tx.s.apply(db)
	db.stats.Commits++
	db.maxIndex++
	return nil
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

func (db *DB) BucketGet(bucket, key string) Value {
	db.mux.RLock()
	v := db.s[bucket][key]
	db.mux.RUnlock()
	if db.opts.CopyOnGet {
		return v.Copy()
	}
	return v
}

func (db *DB) Get(bucket, key string) Value {
	return db.BucketGet(RootBucket, key)
}

func (db *DB) BucketGetObject(bucket, key string, out interface{}) error {
	db.mux.RLock()
	bv := db.s[bucket][key]
	db.mux.RUnlock()
	return db.opts.Unmarshaler(bv, out)
}

func (db *DB) GetObject(key string, out interface{}) error {
	return db.BucketGetObject(RootBucket, key, out)
}

func (db *DB) BucketSet(bucket, key string, value []byte) error {
	return db.Update(func(tx *Tx) error {
		return tx.BucketSet(bucket, key, value)
	})
}

func (db *DB) Set(key string, value []byte) error {
	return db.BucketSet(RootBucket, key, value)
}

func (db *DB) BucketSetObject(bucket, key string, value interface{}) error {
	return db.Update(func(tx *Tx) error {
		return tx.BucketSetObject(bucket, key, value)
	})
}

func (db *DB) SetObject(bucket, key string, value interface{}) error {
	return db.BucketSetObject(RootBucket, key, value)
}

// Compact compacts the database, transactions will be lost, however the counter will still be valid.
func (db *DB) Compact() error {
	db.mux.Lock()
	f, err := ioutil.TempFile("", "jdb-compact")

	defer func() {
		db.mux.Unlock()
		if err != nil {
			f.Close()
			os.Remove(f.Name())
		}
	}()

	if err != nil {
		return err
	}

	if err = db.opts.GetEncoder(f).Encode(&fileTx{
		Index:       db.maxIndex,
		TS:          time.Now().Unix(),
		CompactData: db.s,
	}); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	db.f.Close() // we don't really care at this point

	if err := os.Rename(f.Name(), db.f.Name()); err != nil {
		f.Close()
		log.Panicf("error renaming files, non of the files were overwritten\ndb file: %s\ncompacted db file: %s",
			db.f.Name(), f.Name())
	}

	db.init(f)
	return err
}
