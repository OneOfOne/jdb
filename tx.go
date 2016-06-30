package jdb

import (
	"errors"

	"github.com/missionMeteora/binny.v2"
)

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
	db *DB
	s  storage
	rw bool
}

func (tx *Tx) Get(k string) []byte {
	if v, ok := tx.s[k]; ok {
		return v.Value
	}
	return tx.db.s[k]
}

func (tx *Tx) GetObject(k string, v interface{}) error {
	return binny.Unmarshal(tx.Get(k), v)
}

func (tx *Tx) Set(k string, v []byte) error {
	if !tx.rw {
		return ErrReadOnly
	}
	tx.s[k] = entry{v, entrySet}
	return nil
}

func (tx *Tx) SetObject(k string, v interface{}) error {
	bv, err := binny.Marshal(v)
	if err != nil {
		return err
	}
	return tx.Set(k, bv)
}

func (tx *Tx) Delete(k string) error {
	if !tx.rw {
		return ErrReadOnly
	}
	tx.s[k] = entry{Type: entryDelete}
	return nil
}
