# jdb ![Status](https://img.shields.io/badge/status-alpha-red.svg)

### A file-backed transactional in-memory datastore trying to be ACID compliant.

## Example:

```go
//db, err := jdb.New(fp, &Opts{Backend: GZipJSONBackend})
db, err := jdb.New("db.jdb", nil) // default JSONBackend
if err != nil {
	log.Panic(err)
}

err := db.Update(func(tx *Tx) error {
	return tx.Set("a", []byte("a")) // or
	return tx.Set("a", jdb.Value("a"))
})

// shorthand for the above if you only need to set 1 value.
err := db.Set("a", []byte("a"))
```