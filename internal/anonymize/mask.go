package anonymize

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Masker applies deterministic PII masking using HMAC-SHA256.
type Masker struct {
	salt []byte
}

// New creates a new Masker with the given salt.
func New(salt string) *Masker {
	return &Masker{salt: []byte(salt)}
}

// hmacHex returns the hex-encoded HMAC-SHA256 of value using the masker's salt.
func (m *Masker) hmacHex(value string) string {
	h := hmac.New(sha256.New, m.salt)
	h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil))
}

// MaskColumn deterministically masks a column value based on the column name heuristic.
// Returns the masked value as a string, or the original if no heuristic matches.
func (m *Masker) MaskColumn(columnName string, value string) string {
	colLower := strings.ToLower(columnName)
	hash := m.hmacHex(value)

	switch {
	case colLower == "email" || strings.HasSuffix(colLower, "_email"):
		return fmt.Sprintf("anon_%s@drain.local", hash[:10])

	case colLower == "phone" || colLower == "mobile" ||
		strings.Contains(colLower, "phone") || strings.Contains(colLower, "mobile"):
		return fmt.Sprintf("+1555%s", hash[:7])

	case colLower == "first_name":
		return fmt.Sprintf("User_%s", hash[:6])

	case colLower == "last_name":
		return fmt.Sprintf("Anon_%s", hash[:6])

	case colLower == "name" || strings.HasSuffix(colLower, "_name"):
		return fmt.Sprintf("User_%s", hash[:6])

	case colLower == "password" || colLower == "passwd" ||
		strings.Contains(colLower, "password") || strings.Contains(colLower, "passwd"):
		return "$2a$12$e8rG.e9y1dYv9fD1v1.X7fakehash"

	case colLower == "token" || colLower == "secret" ||
		strings.Contains(colLower, "token") || strings.Contains(colLower, "secret"):
		return fmt.Sprintf("tok_%s", hash[:16])
	}

	return value
}

// ShouldMask returns true if the column name matches a PII heuristic.
func ShouldMask(columnName string) bool {
	colLower := strings.ToLower(columnName)
	piiPatterns := []string{
		"email", "phone", "mobile", "first_name", "last_name",
		"name", "password", "passwd", "token", "secret",
	}
	for _, p := range piiPatterns {
		if strings.Contains(colLower, p) {
			return true
		}
	}
	return false
}
