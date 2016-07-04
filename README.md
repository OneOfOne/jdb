# jdb ![Status](https://img.shields.io/badge/status-beta-red.svg)

### A file-backed ACID in-memory k/v data store.

## FAQ

### Why would I use this over bolt or any other K/V store?

1. The data is always in memory, in a very simple memory layout and it is extremely fast.
2. You can specify the on-disk file format, the default is JSON, however
it is very easy to add your own.
3. You can use the values directly in your code without having to copy them*
4. You can fully replay the database and discard transactions as needed (not implemented yet).

* *modifying values without a copy can result in a race, but the on-disk data won't be corrupted.*
* *compacting the database will remove older transactions.*

### Why shouldn't I use this?

Mainly if your dataset doesn't fit in memory then you're better off with
an mmap'ed k/v store like the excellent [boltdb](https://github.com/boltdb/bolt).

## Benchmarks

```
➤ go test -v -bench=. -benchmem -benchtime=5s
=== RUN   TestDB
--- PASS: TestDB (0.00s)
BenchmarkJDBSameTxReadWrite-8            3000000              2808 ns/op             312 B/op         10 allocs/op
BenchmarkBoltSameTxReadWrite-8           1000000              8171 ns/op            6309 B/op         44 allocs/op

BenchmarkJDBSeparateReadWrite-8          3000000              2935 ns/op             312 B/op         10 allocs/op
BenchmarkBoltSeparateReadWrite-8         1000000             12122 ns/op            9495 B/op         53 allocs/op
PASS
ok      github.com/OneOfOne/jdb 48.915s
```

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