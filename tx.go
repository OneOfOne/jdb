package jdb

import (
	"encoding/gob"
	"errors"
)

func init() {
	gob.Register(&fileTx{})
}

var (
	ErrReadOnly = errors.New("readonly tx")
)

type entry struct {
	Value []byte `json:"v,omitempty"`
	Type  uint8  `json:"t,omitempty"`
}

type storage map[string]entry

type fileTx struct {
	TS   int64   `json:"ts"`
	Data storage `json:"v,omitempty"`
}

type Tx struct {
	db  *DB
	tmp map[string]entry
	rw  bool
}

func (tx *Tx) Get(k string) []byte {
	if v, ok := tx.tmp[k]; ok {
		return v.Value
	}
	return tx.db.s[k]
}

func (tx *Tx) Set(k string, v []byte) error {
	if !tx.rw {
		return ErrReadOnly
	}
	tx.tmp[k] = entry{v, entrySet}
	return nil
}

func (tx *Tx) Delete(k string) error {
	if !tx.rw {
		return ErrReadOnly
	}
	tx.tmp[k] = entry{Type: entryDelete}
	return nil
}
