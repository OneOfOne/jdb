package jdb

import "errors"

const RootBucket = "☢"

var (
	ErrReadOnly = errors.New("readonly tx")
	ErrNilValue = errors.New("value can't be nil")
)

type Value []byte

func (v Value) String() string { return string(v) }
func (v Value) Raw() []byte    { return []byte(v) }
func (v Value) Copy() []byte {
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp
}

type fileTx struct {
	Index     uint64  `json:"idx,omitempty"`
	TS        int64   `json:"ts,omitempty"`
	Changeset *bucket `json:"cs,omitempty"`
	Compact   bool    `json:"compact,omitempty"`
}

type Tx struct {
	BucketTx
}

type bucket struct {
	Buckets map[string]*bucket `json:"buckets,omitempty"`
	Data    map[string]Value   `json:"data,omitempty"`
}

func (b *bucket) Get(key string) Value {
	if b == nil {
		return nil
	}
	return b.Data[key]
}

func (b *bucket) Set(key string, val Value) {
	if b.Data == nil {
		b.Data = map[string]Value{}
	}
	b.Data[key] = val
}

func (b *bucket) Delete(key string) {
	delete(b.Data, key)
}

func (b *bucket) Bucket(name string) *bucket {
	if b.Buckets == nil {
		b.Buckets = map[string]*bucket{}
	}

	nb := b.Buckets[name]
	if nb == nil {
		nb = &bucket{}
		b.Buckets[name] = nb
	}
	return nb
}

func (b *bucket) DeleteBucket(name string) {
	delete(b.Buckets, name)
}

type BucketTx struct {
	tmpBucket  *bucket
	realBucket *bucket
	rw         bool
}

func (b *BucketTx) Get(key string) Value {
	if v := b.tmpBucket.Get(key); v != nil {
		return v
	}
	return b.realBucket.Get(key)
}

func (b *BucketTx) Set(key string, val Value) error {
	if !b.rw {
		return ErrReadOnly
	}
	if val == nil {
		return ErrNilValue
	}
	b.tmpBucket.Set(key, val)
	return nil
}

func (b *BucketTx) Delete(key string) error {
	if !b.rw {
		return ErrReadOnly
	}
	b.tmpBucket.Set(key, nil)
	return nil
}

func (b *BucketTx) ForEach(fn func(key string, val Value) error) error {
	for k, v := range b.tmpBucket.Data {
		if v == nil {
			continue
		}

		if err := fn(k, v); err != nil {
			return err
		}
	}
	if b.realBucket == nil {
		return nil
	}
	for k, v := range b.realBucket.Data {
		if _, ok := b.tmpBucket.Data[k]; ok {
			continue
		}
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (b *BucketTx) Bucket(name string) *BucketTx {
	var rb *bucket
	if b.realBucket != nil {
		rb = b.realBucket.Buckets[name]
	}
	return &BucketTx{
		tmpBucket:  b.tmpBucket.Bucket(name),
		realBucket: rb,
		rw:         b.rw,
	}
}

func (b *BucketTx) DeleteBucket(name string) error {
	if !b.rw {
		return ErrReadOnly
	}

	if b.tmpBucket.Buckets == nil {
		b.tmpBucket.Buckets = map[string]*bucket{}
	}
	b.tmpBucket.Buckets[name] = nil
	return nil
}
