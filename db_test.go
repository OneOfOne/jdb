package jdb_test

import (
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"crypto/sha512"

	"github.com/OneOfOne/jdb"
	"github.com/OneOfOne/jdb/backends/crypto"
	"github.com/boltdb/bolt"
)

var (
	keepTmp           = flag.Bool("k", false, "keep temp files")
	boltDefaultBucket = []byte("bucket")
	key               = sha512.Sum512_256([]byte("hello world"))
	tmpDir            string
)

func TestMain(m *testing.M) {
	log.SetFlags(log.Lshortfile)
	flag.Parse()

	dir, err := ioutil.TempDir("", "jdb-")
	if err != nil {
		log.Fatal(err)
	}
	tmpDir = dir

	if *keepTmp {
		log.Println("temp dir:", dir)
	}

	code := m.Run()
	if !*keepTmp {
		os.RemoveAll(dir)
	}
	os.Exit(code)
}

func getJDB(tb testing.TB, fp string, be func() jdb.Backend) *jdb.DB {
	db, err := jdb.New(fp, &jdb.Opts{Backend: be})
	if err != nil {
		tb.Fatal(fp, err)
	}
	return db
}

func TestDB(t *testing.T) {
	fp := filepath.Join(tmpDir, "gzip-json.jdb")
	db := getJDB(t, fp, jdb.GZipLevelJSONBackend(9))

	//defer os.RemoveFile(db.f.Name())
	db.Update(func(tx *jdb.Tx) error {
		return tx.Set("a", jdb.Value("a"))
	})

	if err := db.Read(func(tx *jdb.Tx) error {
		if tx.Get("a") == nil {
			t.Error("couldn't get a")
		}
		return tx.Set("a", []byte("a"))
	}); err != jdb.ErrReadOnly {
		t.Fatal("expected ErrReadOnly, got", err)
	}

	db.Update(func(tx *jdb.Tx) error {
		tx.Set("b", []byte("b"))
		return tx.Delete("a")
	})

	db.Read(func(tx *jdb.Tx) error {
		if tx.Get("a") != nil {
			t.Error("got a when we shouldn't have")
		}
		return nil
	})

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	var err error
	if db, err = jdb.New(fp, &jdb.Opts{Backend: jdb.GZipJSONBackend}); err != nil {
		t.Fatal(err)
	}

	db.Update(func(tx *jdb.Tx) error {
		tx.Bucket("parent").Bucket("child").Set("the punisher", jdb.Value("coolest hero"))
		return tx.Set("c", []byte("c"))
	})

	db.Read(func(tx *jdb.Tx) error {
		if tx.Get("b") == nil {
			t.Error("coudln't load b :(")
		}
		b := tx.Bucket("parent").Bucket("child")
		if b.Get("the punisher") == nil {
			t.Error("NOOOOOOO")
		}
		return nil
	})

	db.Close()
}

func TestCryptoBackend(t *testing.T) {
	fp := filepath.Join(tmpDir, "crypto-gzip-json.jdb")
	opts := &jdb.Opts{Backend: crypto.AESBackend(jdb.GZipJSONBackend, key[:])}
	db, err := jdb.New(fp, opts)
	if err != nil {
		t.Fatal(fp, err)
	}
	for i := 0; i < 10; i++ {
		bn := "bucket-" + strconv.Itoa(i)
		if err := db.Update(func(tx *jdb.Tx) error {
			b := tx.Bucket(bn)
			for i := 0; i < 100; i++ {
				kn := strconv.Itoa(i)
				b.Set(kn, jdb.Value(kn))
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	db, err = jdb.New(fp, opts)
	if err != nil {
		t.Fatal(fp, err)
	}
	db.Read(func(tx *jdb.Tx) error {
		for i := 0; i < 10; i++ {
			bn := "bucket-" + strconv.Itoa(i)
			b := tx.Bucket(bn)
			for i := 0; i < 100; i++ {
				kn := strconv.Itoa(i)
				if v := b.Get(kn); v.String() != kn {
					t.Errorf("expected %s, got %s", kn, v)
				}
			}
		}
		return nil
	})
	db.Close()
}

func benchJDB(b *testing.B, name string, sameTx bool, be func() jdb.Backend) {
	name = strconv.Itoa(rand.Int()) + "-" + name
	db, err := jdb.New(filepath.Join(tmpDir, name), nil)
	if err != nil {
		b.Fatal(name, err)
	}
	defer db.Close()
	var testFn func(pb *testing.PB)
	if sameTx {
		testFn = func(pb *testing.PB) {
			for pb.Next() {
				db.Update(func(tx *jdb.Tx) error {
					tx.Set("value", []byte("value"))
					if string(tx.Get("value")) != "value" {
						b.Fatal(name, "something went wrong")
					}
					return nil
				})
			}
		}
	} else {
		testFn = func(pb *testing.PB) {
			for pb.Next() {
				db.Set("value", []byte("value"))
				if string(db.Get("value")) != "value" {
					b.Fatal("something went wrong")
				}
			}
		}
	}
	b.ResetTimer()
	b.RunParallel(testFn)
}

func BenchmarkJDBSameTxReadWriteJSON(b *testing.B) {
	benchJDB(b, "SameTxReadWriteJSON", true, jdb.JSONBackend)
}

func BenchmarkJDBSameTxReadWriteGzipJSON(b *testing.B) {
	benchJDB(b, "SameTxReadWriteGzipJSON", true, jdb.GZipJSONBackend)
}

func BenchmarkJDBSameTxReadWriteCryptoJSON(b *testing.B) {
	be := crypto.AESBackend(jdb.JSONBackend, key[:])
	benchJDB(b, "SameTxReadWriteCryptoJSON", true, be)
}

func BenchmarkJDBSameTxReadWriteCryptoGzipJSON(b *testing.B) {
	be := crypto.AESBackend(jdb.GZipJSONBackend, key[:])
	benchJDB(b, "SameTxReadWriteCryptoGzipJSON", true, be)
}

func initBolt(name string) (*bolt.DB, error) {
	db, err := bolt.Open(filepath.Join(tmpDir, name), 0644, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucket(boltDefaultBucket)
		return nil
	})
	return db, err
}

func BenchmarkBoltSameTxReadWrite(b *testing.B) {
	db, err := initBolt("bench-rwtx.bolt")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db.Update(func(tx *bolt.Tx) error {
				bkt := tx.Bucket(boltDefaultBucket)
				bkt.Put([]byte("value"), []byte("value"))
				if string(bkt.Get([]byte("value"))) != "value" {
					b.Fatal("something went wrong")
				}
				return nil
			})
		}
	})
}

func BenchmarkJDBSeparateReadWriteJSON(b *testing.B) {
	benchJDB(b, "SeparateTxReadWriteJSON", false, jdb.JSONBackend)
}

func BenchmarkJDBSeparateReadWriteGzipJSON(b *testing.B) {
	benchJDB(b, "SeparateTxReadWriteGzipJSON", false, jdb.GZipJSONBackend)
}

func BenchmarkJDBSeparateReadWriteCryptoJSON(b *testing.B) {
	be := crypto.AESBackend(jdb.JSONBackend, key[:])
	benchJDB(b, "SeparateTxReadWriteCryptoJSON", false, be)
}

func BenchmarkJDBSeparateReadWriteCryptoGzipJSON(b *testing.B) {
	be := crypto.AESBackend(jdb.GZipJSONBackend, key[:])
	benchJDB(b, "SeparateTxReadWriteCryptoGzipJSON", false, be)
}

func BenchmarkBoltSeparateReadWrite(b *testing.B) {
	db, err := initBolt("bench-rw.bolt")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(boltDefaultBucket).Put([]byte("value"), []byte("value"))
			})
			var val string
			db.View(func(tx *bolt.Tx) error {
				val = string(tx.Bucket(boltDefaultBucket).Get([]byte("value")))
				return nil
			})
			if val != "value" {
				b.Fatal("something went wrong")
			}
		}
	})
}
