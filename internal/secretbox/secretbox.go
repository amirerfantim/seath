// Package secretbox encrypts small secrets (NATS preset passwords and tokens)
// before they are written to Postgres, so a database reader sees ciphertext.
//
// Values are AES-256-GCM sealed and stored as "enc:v1:" + base64(nonce||ct).
// The caller passes associated data (for example the preset name and column)
// so a ciphertext copied into another row or column fails to open.
package secretbox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// Prefix marks an encrypted value. Anything without it is legacy plaintext.
const Prefix = "enc:v1:"

// KeyEnv is the environment variable that holds the base64 32-byte key.
const KeyEnv = "SEATH_SECRET_KEY"

// ErrNoKey is returned when an encrypted value is read without a key.
var ErrNoKey = errors.New("value is encrypted but " + KeyEnv + " is not set")

// Box seals and opens values with one AES-256-GCM key.
type Box struct {
	aead cipher.AEAD
}

// ParseKey builds a Box from a base64-encoded 32-byte key, as produced by
// `openssl rand -base64 32`. Standard and URL-safe alphabets are accepted.
func ParseKey(encoded string) (*Box, error) {
	encoded = strings.TrimSpace(encoded)
	var key []byte
	var err error
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if key, err = enc.DecodeString(encoded); err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%s must be base64: %w", KeyEnv, err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("%s must decode to 32 bytes, got %d (generate one with `openssl rand -base64 32`)", KeyEnv, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Box{aead: aead}, nil
}

// IsEncrypted reports whether v carries the ciphertext prefix.
func IsEncrypted(v string) bool { return strings.HasPrefix(v, Prefix) }

// Seal encrypts plaintext bound to aad. An empty plaintext stays empty, so
// "no password" is still visible as such and needs no key to read.
func (b *Box) Seal(plaintext, aad string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	out := b.aead.Seal(nonce, nonce, []byte(plaintext), []byte(aad))
	return Prefix + base64.StdEncoding.EncodeToString(out), nil
}

// Open decrypts a value written by Seal with the same aad. A value without the
// prefix is returned unchanged (legacy plaintext).
func (b *Box) Open(stored, aad string) (string, error) {
	if !IsEncrypted(stored) {
		return stored, nil
	}
	if b == nil {
		return "", ErrNoKey
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, Prefix))
	if err != nil {
		return "", fmt.Errorf("decode encrypted value: %w", err)
	}
	n := b.aead.NonceSize()
	if len(raw) < n {
		return "", errors.New("encrypted value is too short")
	}
	pt, err := b.aead.Open(nil, raw[:n], raw[n:], []byte(aad))
	if err != nil {
		return "", fmt.Errorf("decrypt value (wrong %s?): %w", KeyEnv, err)
	}
	return string(pt), nil
}
