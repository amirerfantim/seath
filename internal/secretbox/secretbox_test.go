package secretbox

import (
	"encoding/base64"
	"strings"
	"testing"
)

func testBox(t *testing.T, fill byte) *Box {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = fill
	}
	b, err := ParseKey(base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSealOpenRoundTrip(t *testing.T) {
	b := testBox(t, 1)
	ct, err := b.Seal("s3cret", "recorder_presets/prod/password")
	if err != nil {
		t.Fatal(err)
	}
	if !IsEncrypted(ct) || strings.Contains(ct, "s3cret") {
		t.Fatalf("value not encrypted: %q", ct)
	}
	pt, err := b.Open(ct, "recorder_presets/prod/password")
	if err != nil || pt != "s3cret" {
		t.Fatalf("Open = %q, %v", pt, err)
	}
}

func TestSealUsesFreshNonce(t *testing.T) {
	b := testBox(t, 1)
	a, _ := b.Seal("same", "aad")
	c, _ := b.Seal("same", "aad")
	if a == c {
		t.Fatal("two seals of the same value are identical")
	}
}

func TestOpenRejectsMovedCiphertext(t *testing.T) {
	b := testBox(t, 1)
	ct, _ := b.Seal("s3cret", "recorder_presets/prod/password")
	if _, err := b.Open(ct, "recorder_presets/other/password"); err == nil {
		t.Fatal("ciphertext opened under another preset's aad")
	}
	if _, err := b.Open(ct, "recorder_presets/prod/token"); err == nil {
		t.Fatal("ciphertext opened under another column's aad")
	}
}

func TestOpenRejectsWrongKey(t *testing.T) {
	ct, _ := testBox(t, 1).Seal("s3cret", "aad")
	if _, err := testBox(t, 2).Open(ct, "aad"); err == nil {
		t.Fatal("ciphertext opened with another key")
	}
}

func TestEmptyAndLegacyValues(t *testing.T) {
	b := testBox(t, 1)
	if ct, _ := b.Seal("", "aad"); ct != "" {
		t.Fatalf("empty value sealed to %q", ct)
	}
	if pt, err := b.Open("legacy-plain", "aad"); err != nil || pt != "legacy-plain" {
		t.Fatalf("legacy plaintext = %q, %v", pt, err)
	}
	var none *Box
	if pt, err := none.Open("legacy-plain", "aad"); err != nil || pt != "legacy-plain" {
		t.Fatalf("legacy plaintext without key = %q, %v", pt, err)
	}
	ct, _ := b.Seal("s3cret", "aad")
	if _, err := none.Open(ct, "aad"); err != ErrNoKey {
		t.Fatalf("encrypted value without key: err = %v, want ErrNoKey", err)
	}
}

func TestParseKeyValidation(t *testing.T) {
	for _, bad := range []string{"", "not base64!!", base64.StdEncoding.EncodeToString(make([]byte, 16))} {
		if _, err := ParseKey(bad); err == nil {
			t.Errorf("ParseKey(%q) accepted a bad key", bad)
		}
	}
	key := make([]byte, 32)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawURLEncoding} {
		if _, err := ParseKey(" " + enc.EncodeToString(key) + "\n"); err != nil {
			t.Errorf("ParseKey rejected a valid key: %v", err)
		}
	}
}
