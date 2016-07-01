package jdb

import "errors"

const RootBucket = "☢"

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

type txBucket map[string]entry

type storage map[string]txBucket

func (s storage) apply(db *DB) {
	for bn, b := range s {
		if b == nil {
			delete(db.s, bn)
			continue
		}
		m := db.s[bn]
		if m == nil {
			m = Bucket{}
			db.s[bn] = m
		}
		for k, v := range b {
			switch v.Type {
			case entrySet:
				if db.opts.CopyOnSet {
					m[k] = Value(v.Value.Copy())
				} else {
					m[k] = v.Value
				}
			case entryDelete:
				delete(m, k)
			}
		}
	}

}

type fileTx struct {
	Index       uint64            `json:"idx,omitempty"`
	TS          int64             `json:"ts,omitempty"`
	CompactData map[string]Bucket `json:"compactData,omitempty"`
	Data        storage           `json:"data,omitempty"`
}

type Tx struct {
	db *DB
	s  storage
	rw bool
}

func (tx *Tx) BucketGet(bucket, key string) Value {
	var out Value
	if b, ok := tx.s[bucket]; ok {
		out = b[key].Value
	} else {
		out = tx.db.s[bucket][key]
	}
	if tx.db.opts.CopyOnGet {
		return out.Copy()
	}
	return out
}

func (tx *Tx) Get(key string) Value {
	return tx.BucketGet(RootBucket, key)
}

func (tx *Tx) BucketGetObject(bucket, key string, out interface{}) error {
	var v Value
	if b, ok := tx.s[bucket]; ok {
		v = b[key].Value
	} else {
		v = tx.db.s[bucket][key]
	}
	return tx.db.opts.Unmarshaler(v, out)
}

func (tx *Tx) GetObject(key string, out interface{}) error {
	return tx.BucketGetObject(RootBucket, key, out)
}

func (tx *Tx) BucketSet(bucket, key string, value []byte) error {
	if !tx.rw {
		return ErrReadOnly
	}
	b := tx.s[bucket]
	if b == nil {
		b = txBucket{}
		tx.s[bucket] = b
	}

	b[key] = entry{value, entrySet}
	return nil
}

func (tx *Tx) Set(key string, value []byte) error {
	return tx.BucketSet(RootBucket, key, value)
}

func (tx *Tx) BucketSetObject(bucket, key string, value interface{}) error {
	bv, err := tx.db.opts.Marshaler(value)
	if err != nil {
		return err
	}
	return tx.BucketSet(bucket, key, bv)
}

func (tx *Tx) SetObject(key string, value interface{}) error {
	return tx.BucketSetObject(RootBucket, key, value)
}

func (tx *Tx) BucketDelete(bucket, key string) error {
	if !tx.rw {
		return ErrReadOnly
	}
	b := tx.s[bucket]
	if b == nil {
		b = txBucket{}
		tx.s[bucket] = b
	}
	b[key] = entry{Type: entryDelete}
	return nil
}

func (tx *Tx) Delete(key string) error { return tx.BucketDelete(RootBucket, key) }

func (tx *Tx) DeleteBucket(bucket string) error {
	if !tx.rw {
		return ErrReadOnly
	}
	tx.s[bucket] = nil
	return nil
}
