# jdb [![MIT licensed](https://img.shields.io/badge/license-apache-blue.svg)](https://raw.githubusercontent.com/OneOfOne/jdb/master/LICENSE) ![Status](https://img.shields.io/badge/status-beta-red.svg)

**A file-backed ACID in-memory k/v data store.**

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
➤ go test -benchmem -bench=.  -v  -benchtime=3s
=== RUN   TestDB
--- PASS: TestDB (0.00s)
=== RUN   TestCryptoBackend
--- PASS: TestCryptoBackend (0.00s)
BenchmarkJDBSameTxReadWriteJSON-8               2000000     2898 ns/op             312 B/op         10 allocs/op
BenchmarkJDBSameTxReadWriteGzipJSON-8           2000000     3027 ns/op             312 B/op         10 allocs/op
BenchmarkJDBSameTxReadWriteCryptoJSON-8         1000000     3077 ns/op             312 B/op         10 allocs/op
BenchmarkJDBSameTxReadWriteCryptoGzipJSON-8     2000000     2908 ns/op             312 B/op         10 allocs/op

BenchmarkBoltSameTxReadWrite-8                   500000     8682 ns/op            6309 B/op         44 allocs/op

BenchmarkJDBSeparateReadWriteJSON-8             1000000     3000 ns/op             312 B/op         10 allocs/op
BenchmarkJDBSeparateReadWriteGzipJSON-8         1000000     3014 ns/op             312 B/op         10 allocs/op
BenchmarkJDBSeparateReadWriteCryptoJSON-8       2000000     3020 ns/op             312 B/op         10 allocs/op
BenchmarkJDBSeparateReadWriteCryptoGzipJSON-8   1000000     3000 ns/op             312 B/op         10 allocs/op

BenchmarkBoltSeparateReadWrite-8                 500000    16040 ns/op           12416 B/op         53 allocs/op

PASS
ok      github.com/OneOfOne/jdb 60.457s

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

## License

Apache v2.0 (see [LICENSE](https://raw.githubusercontent.com/OneOfOne/jdb/master/LICENSE) file).

Copyright 2016-2016 Ahmed <[OneOfOne](https://github.com/OneOfOne/)> W.
