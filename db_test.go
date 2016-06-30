package jdb

import "testing"
import "io/ioutil"

func TestDB(t *testing.T) {
	fp, err := ioutil.TempFile("", "jdb-")
	if err != nil {
		t.Fatal(err)
	}
	fp.Close()
	defer os.RemoveFile(fp.Name())
	db, err := New(fp.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
		return tx.Delete("a")
	})

	db.Read(func(tx *Tx) error {
		if tx.Get("a") != nil {
			t.Error("got a when we shouldn't have")
		}
		return nil
	})

}
