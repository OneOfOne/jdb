package jdb

import "errors"

var (
	ErrReadOnly = errors.New("readonly tx")
)

type entry struct {
	Value string `json:"v,omitempty"`
	Type  uint8  `json:"t,omitempty"`
}

type storage map[string]*entry

type fileTx struct {
	TS   int64   `json:"ts"`
	Data storage `json:"data,omitempty"`
}

type Tx struct {
	db  *DB
	tmp map[string]*entry
	rw  bool
}

func (tx *Tx) Get(k string) string {
	if tx.rw {
		if v := tx.tmp[k]; v != nil {
			return v.Value
		}
	}
	return tx.db.s[k]
}

func (tx *Tx) Set(k string, v string) error {
	if !tx.rw {
		return ErrReadOnly
	}
	tx.tmp[k] = &entry{v, entrySet}
	return nil
}

func (tx *Tx) Delete(k string) error {
	if !tx.rw {
		return ErrReadOnly
	}
	tx.tmp[k] = &entry{Type: entryDelete}
	return nil
}
