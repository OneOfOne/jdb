package jdb

import (
	"errors"

	"github.com/missionMeteora/binny.v2"
)

var (
	ErrReadOnly = errors.New("readonly tx")
)

type Value []byte

func (v Value) String() string { return string(v) }
func (v Value) Raw() []byte    { return []byte(v) }
func (v Value) Copy() []byte {
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp
}

type entry struct {
	Value Value `json:"value,omitempty"`
	Type  uint8 `json:"type,omitempty"`
}

type storage map[string]entry

type fileTx struct {
	Index       uint64           `json:"idx,omitempty"`
	TS          int64            `json:"ts,omitempty"`
	CompactData map[string]Value `json:"compactData,omitempty"`
	Data        storage          `json:"data,omitempty"`
}

type Tx struct {
	db *DB
	s  storage
	rw bool
}

func (tx *Tx) Get(k string) Value {
	var out Value
	if v, ok := tx.s[k]; ok {
		out = v.Value
	} else {
		out = tx.db.s[k]
	}
	if tx.db.opts.CopyOnGet {
		return out.Copy()
	}
	return out
}

func (tx *Tx) GetObject(k string, v interface{}) error {
	var out Value
	if v, ok := tx.s[k]; ok {
		out = v.Value
	} else {
		out = tx.db.s[k]
	}
	return binny.Unmarshal(out, v)
}

func (tx *Tx) Set(k string, v []byte) error {
	if !tx.rw {
		return ErrReadOnly
	}
	tx.s[k] = entry{Value(v), entrySet}
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
