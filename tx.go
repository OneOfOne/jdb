package jdb

const RootBucket = "☢"

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

func (b *bucket) GetAll() map[string]Value {
	out := make(map[string]Value, len(b.Data))
	for k, v := range b.Data {
		out[k] = v
	}
	return out
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

// GetAll returns a map of all the key/values in *this* bucket.
func (b *BucketTx) GetAll() map[string]Value {
	out := make(map[string]Value)

	if rb := b.realBucket; rb != nil {
		for k, v := range rb.Data {
			out[k] = v
		}
	}

	for k, v := range b.tmpBucket.Data {
		out[k] = v
	}

	return out
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

// Bucket returns a bucket with the specified name, creating it if it doesn't already exist.
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

// Buckets returns a slice of child buckets.
func (b *BucketTx) Buckets() []string {
	var out []string

	tb := b.tmpBucket.Buckets

	for bn, bv := range tb {
		if bv == nil {
			continue
		}
		out = append(out, bn)
	}

	if rb := b.realBucket; rb != nil {
		for bn := range rb.Buckets {
			if _, ok := tb[bn]; ok { // ignore buckets that got modified in the tx
				continue
			}
			out = append(out, bn)
		}
	}

	return out
}

// DeleteBucket opens a portal into a 2D universe full of wonders.
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
