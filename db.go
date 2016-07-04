package jdb

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

var (
	ErrReadOnly           = errors.New("readonly")
	ErrNotImpl            = errors.New("not implemented")
	ErrNilValue           = errors.New("value can't be nil")
	ErrClosed             = errors.New("db is closed, you may access read-only operations")
	ErrMissingMarshaler   = errors.New("missing marshaler")
	ErrMissingUnmarshaler = errors.New("missing unmarshaler")
)

//type Bucket map[string]Value

type DB struct {
	mux sync.RWMutex
	f   *os.File

	root bucket

	maxIndex uint64

	txPool sync.Pool

	opts  Opts
	be    Backend
	stats struct {
		Rollbacks int64
		Commits   int64
	}
}

func New(fp string, opts *Opts) (*DB, error) {
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = &Opts{}
	}
	if opts.Backend == nil {
		opts.Backend = JSONBackend
	}

	db := &DB{
		opts: *opts,
		be:   opts.Backend(),
	}

	if err = db.init(f); err != nil {
		return nil, err
	}

	db.txPool.New = func() interface{} { return db.createTx() }

	if err = db.load(); err != nil {
		return nil, err
	}
	db.maxIndex++
	_, err = db.f.Seek(0, os.SEEK_END)
	return db, err
}

func (db *DB) init(f *os.File) error {
	db.f = f
	return db.be.Init(f, f)
}

func (db *DB) load() error {
	st, err := db.f.Stat()
	if err != nil {
		return err
	}

	if st.Size() == 0 {
		return nil
	}

	db.f.Seek(0, os.SEEK_SET)
	for {
		var tx fileTx
		err := db.be.Decode(&tx)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}

		db.applyTx(tx.Changeset, &db.root)
		db.maxIndex = tx.Index
	}
}

func (db *DB) createTx() *Tx {
	return &Tx{
		BucketTx: BucketTx{
			db: db,

			tmpBucket:  &bucket{},
			realBucket: &db.root,
		},
	}
}

func (db *DB) getTx(rw bool) *Tx {
	tx := db.txPool.Get().(*Tx)
	tx.rw = rw
	return tx
}

func (db *DB) putTx(tx *Tx) {
	if tb := tx.tmpBucket; tb != nil {
		for k := range tb.Buckets {
			delete(tb.Buckets, k)
		}
		for k := range tb.Data {
			delete(tb.Data, k)
		}
	}
	db.txPool.Put(tx)
}

func (db *DB) applyTx(src, dst *bucket) {
	for k, v := range src.Data {
		if v == nil {
			dst.Delete(k)
		} else {
			if db.opts.CopyOnSet {
				v = v.Copy()
			}
			dst.Set(k, v)
		}
	}

	for bn, b := range src.Buckets {
		if b == nil {
			dst.DeleteBucket(bn)
		} else {
			db.applyTx(b, dst.Bucket(bn))
		}
	}
}

func (db *DB) writeTx(tx *Tx) error {
	curPos, err := db.f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	if err := db.be.Encode(&fileTx{
		Index:     db.maxIndex,
		TS:        time.Now().Unix(),
		Changeset: tx.tmpBucket,
	}); err != nil {
		db.stats.Rollbacks++
		db.f.Truncate(curPos)
		return err
	}

	if err := db.be.Flush(); err != nil {
		db.stats.Rollbacks++
		db.f.Truncate(curPos)
		return err
	}

	if err := db.f.Sync(); err != nil {
		db.stats.Rollbacks++
		db.f.Truncate(curPos)
		return err
	}

	db.applyTx(tx.tmpBucket, &db.root)
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
	if db.isClosed() {
		return ErrClosed
	}
	if err := fn(tx); err != nil {
		db.stats.Rollbacks++
		return err
	}
	return db.writeTx(tx)
}

// Get is a shorthand access a value in an optional bucket chain.
//	Example: v := db.Get("name", "users", "user-id-1")
func (db *DB) Get(key string, bucket ...string) Value {
	db.mux.RLock()
	defer db.mux.RUnlock()
	b := &db.root
	for _, bn := range bucket {
		if b = b.Buckets[bn]; b == nil {
			return nil
		}
	}
	return b.Get(key)
}

func (db *DB) GetObject(key string, out interface{}, bucket ...string) error {
	v := db.Get(key, bucket...)
	return db.be.Unmarshal(v, out)
}

// Set is a shorthand for an Update call with an optional Bucket chain.
//	Example: v := db.Set("name", Value("Moonknight"), "users", "user-id-1")
func (db *DB) Set(key string, val []byte, bucket ...string) error {
	return db.Update(func(tx *Tx) error {
		b := tx.tmpBucket
		for _, bn := range bucket {
			b = b.Bucket(bn)
		}
		b.Set(key, val)
		return nil
	})
}

func (db *DB) SetObject(key string, val interface{}, bucket ...string) error {
	v, err := db.be.Marshal(val)
	if err != nil {
		return err
	}
	return db.Set(key, v, bucket...)
}

func (db *DB) isClosed() bool {
	return int(db.f.Fd()) < 0
}

func (db *DB) Close() error {
	db.mux.Lock()
	if c, ok := db.be.(io.Closer); ok {
		c.Close()
	}
	err := db.f.Close()
	db.mux.Unlock()
	return err
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

	cp := db.opts.Backend()

	if err = cp.Init(f, f); err != nil {
		return err
	}

	if err = cp.Encode(&fileTx{
		Index:     db.maxIndex,
		TS:        time.Now().Unix(),
		Changeset: &db.root,
		Compact:   true,
	}); err != nil {
		return err
	}

	if err = cp.Flush(); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	db.f.Close() // we don't really care at this point

	if err := os.Rename(f.Name(), db.f.Name()); err != nil {
		f.Close()
		return &CompactError{f.Name(), db.f.Name(), err}
	}

	db.be = cp
	return err
}

type CompactError struct {
	OldPath string
	NewPath string
	Err     error
}

func (ce *CompactError) Error() string {
	return fmt.Sprintf("rename error (%v), old path: %s, new path: %s", ce.Err, ce.OldPath, ce.NewPath)
}
