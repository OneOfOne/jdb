# jdb ![Status](https://img.shields.io/badge/status-beta-red.svg)

### A file-backed ACID in-memory k/v data store.

## FAQ

### Why would I use this over bolt or any other K/V store?

1. The data is always in memory, in a very simple memory layout and it is extremely fast.
2. You can specify the on-disk file format, the default is JSON, however
it is very easy to add your own.
3. You can use the values directly in your code without having to copy them*
4. You can fully replay the database and discard transactions as needed (not implemented yet).

* <small>modifying values without a copy can result in a race, but the on-disk data won't be corrupted.</small>
* <small>compacting the database will remove older transactions.</small>

### Why shouldn't I use this?

Mainly if your dataset doesn't fit in memory then you're better off with
an mmap'ed k/v store like the excellent [boltdb](https://github.com/boltdb/bolt).

## Examples

```go
//db, err := jdb.New(fp, &Opts{Backend: GZipJSONBackend})
db, err := jdb.New("db.jdb", nil) // default JSONBackend
if err != nil {
	log.Panic(err)
}

defer db.Close()

err := db.Update(func(tx *Tx) error {
	return tx.Set("a", []byte("a")) // or
	// return tx.Set("a", jdb.Value("a"))
})

// shorthand for a single
err := db.SetObject("map in a sub-bucket", map[string]bool{
	"isCool": true,
}, "parent bucket", "a bucket under the parent bucket")
```

## TODO

* Replay / Filter support.
* Per-bucket unique ID generation.
* Archiving support.