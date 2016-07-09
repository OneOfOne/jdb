package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"

	"github.com/OneOfOne/jdb"
)

type writer struct {
	s cipher.Stream
	w io.Writer
}

func (w writer) Write(src []byte) (n int, err error) {
	w.s.XORKeyStream(src, src)
	return w.w.Write(src)
}

type reader struct {
	s cipher.Stream
	r io.Reader
}

func (r reader) Read(dst []byte) (n int, err error) {
	if n, err = r.r.Read(dst); err != nil {
		return
	}
	r.s.XORKeyStream(dst, dst[:n])
	return n, err
}

type aesBackend struct {
	be  jdb.Backend
	key []byte
}

func (be aesBackend) Init(w io.Writer, r io.Reader) error {
	block, err := aes.NewCipher(be.key)
	if err != nil {
		return err
	}
	iv := make([]byte, aes.BlockSize)
	switch _, err := io.ReadFull(r, iv); err {
	case nil:
	case io.EOF:
		if _, err = io.ReadFull(rand.Reader, iv); err != nil {
			return fmt.Errorf("error reading iv: %v", err)
		}
		if _, err := w.Write(iv); err != nil {
			return fmt.Errorf("error writing iv: %v", err)
		}
	default:
		return err
	}
	w = writer{cipher.NewCFBEncrypter(block, iv), w}
	r = reader{cipher.NewCFBDecrypter(block, iv), r}
	return be.be.Init(w, r)
}

func (be aesBackend) Flush() error {
	return be.be.Flush()
}

func (be aesBackend) Encode(v interface{}) error                 { return be.be.Encode(v) }
func (be aesBackend) Decode(v interface{}) error                 { return be.be.Decode(v) }
func (be aesBackend) Marshal(in interface{}) ([]byte, error)     { return be.be.Marshal(in) }
func (be aesBackend) Unmarshal(in []byte, out interface{}) error { return be.be.Unmarshal(in, out) }

// AESBackend returns a backend that encrypts all data with AES CFB mode.
// The AES strength depends on the size of the key,
// 16, 24 or 32 bytes to select AES-128, AES-192, or AES-256.
func AESBackend(be func() jdb.Backend, key []byte) func() jdb.Backend {
	return func() jdb.Backend {
		return aesBackend{be(), key}
	}
}
