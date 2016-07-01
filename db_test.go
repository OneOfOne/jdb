package jdb

import (
	"io/ioutil"
	"log"
	"testing"
)

func TestDB(t *testing.T) {
	fp, err := ioutil.TempFile("", "jdb-")
	if err != nil {
		t.Fatal(err)
	}
	fp.Close()
	//defer os.Remove(fp.Name())

	log.Println(fp.Name())
	db, err := New(fp.Name(), nil)
	if err != nil {
		t.Fatal(err)
	}

	//defer os.RemoveFile(db.f.Name())
	db.Update(func(tx *Tx) error {
		return tx.Set("a", []byte("a"))
	})

	if err := db.Read(func(tx *Tx) error {
		if tx.Get("a") == nil {
			t.Error("couldn't get a")
		}
		return tx.Set("a", []byte("a"))
	}); err != ErrReadOnly {
		t.Fatal("expected ErrReadOnly, got", err)
	}

	db.Update(func(tx *Tx) error {
		tx.Set("b", []byte("b"))
		return tx.Delete("a")
	})

	db.Read(func(tx *Tx) error {
		if tx.Get("a") != nil {
			t.Error("got a when we shouldn't have")
		}
		return nil
	})

	if err := db.Compact(nil); err != nil {
		t.Fatal(err)
	}
	db.Close()

	if db, err = New(fp.Name(), nil); err != nil {
		t.Fatal(err)
	}

	db.Read(func(tx *Tx) error {
		if tx.Get("b") == nil {
			t.Error("coudln't load b :(")
		}
		return nil
	})

	db.Update(func(tx *Tx) error {
		return tx.Set("c", []byte("c"))
	})
	db.Close()
}
