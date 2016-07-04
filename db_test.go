package jdb_test

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/OneOfOne/jdb"
)

var keepTmp = flag.Bool("k", false, "keep temp files")

func init() {
	log.SetFlags(log.Lshortfile)
	flag.Parse()
}

func TestDB(t *testing.T) {
	dir, err := ioutil.TempDir("", "jdb-")
	if err != nil {
		t.Fatal(err)
	}
	fp := filepath.Join(dir, "gzip-json.jdb")

	if *keepTmp {
		log.Println("temp file:", fp)
	} else {
		defer os.RemoveAll(dir)
	}

	db, err := jdb.New(fp, &jdb.Opts{Backend: jdb.GZipJSONBackend})
	if err != nil {
		t.Fatal(err)
	}

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
