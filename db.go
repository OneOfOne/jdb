package jdb

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/missionMeteora/binny.v2"
)

const (
	entrySet = iota
	entryDelete
)

type DB struct {
	mux sync.RWMutex
	f   *os.File

	s map[string][]byte

	txPool sync.Pool

	encodeFn func(tx *fileTx) error

	stats struct {
		Rollbacks int64
		Commits   int64
	}
}

func New(fp string) (*DB, error) {
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(f)
	db := &DB{
		f:        f,
		encodeFn: func(tx *fileTx) error { return enc.Encode(tx) },
		s:        map[string][]byte{},
	}
	db.txPool.New = func() interface{} { return &Tx{db: db, tmp: storage{}} }
	//db.load()

	return db, nil
}

func (db *DB) getTx(rw bool) *Tx {
	tx := db.txPool.Get().(*Tx)
	tx.rw = rw
	return tx
}

func (db *DB) putTx(tx *Tx) {
	for k := range tx.tmp {
		delete(tx.tmp, k)
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

	if err := db.encodeFn(&fileTx{time.Now().Unix(), tx.tmp}); err != nil {
		db.stats.Rollbacks++
		db.f.Seek(curPos, os.SEEK_SET)
		return err
	}
	for k, v := range tx.tmp {
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
