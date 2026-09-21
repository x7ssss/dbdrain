package anonymize

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
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

// hmacBytes returns the raw HMAC-SHA256 bytes of value.
func (m *Masker) hmacBytes(value string) []byte {
	h := hmac.New(sha256.New, m.salt)
	h.Write([]byte(value))
	return h.Sum(nil)
}

// hmacHex returns the hex-encoded HMAC-SHA256 of value using the masker's salt.
func (m *Masker) hmacHex(value string) string {
	return hex.EncodeToString(m.hmacBytes(value))
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

// ApplyTransform applies a named config transform to value, deterministically.
// Known transforms:
//   - "fake_email"       → anon_<hmac[:10]>@drain.local
//   - "redact"           → [REDACTED]
//   - "uuid_remap"       → deterministic UUID v4 (RFC 4122) from HMAC
//   - "hmac_phone"       → +1555<hmac[:7]>
//   - "hmac_name"        → User_<hmac[:6]>
//   - "redact_password"  → bcrypt placeholder
//   - "hmac_token"       → tok_<hmac[:16]>
//
// Unknown transforms fall back to the automatic heuristic (MaskColumn).
func (m *Masker) ApplyTransform(transform string, columnName string, value string) string {
	switch transform {
	case "fake_email":
		return fmt.Sprintf("anon_%s@drain.local", m.hmacHex(value)[:10])
	case "redact":
		return "[REDACTED]"
	case "uuid_remap":
		return m.UUIDRemap(value)
	case "hmac_phone":
		return fmt.Sprintf("+1555%s", m.hmacHex(value)[:7])
	case "hmac_name":
		return fmt.Sprintf("User_%s", m.hmacHex(value)[:6])
	case "redact_password":
		return "$2a$12$e8rG.e9y1dYv9fD1v1.X7fakehash"
	case "hmac_token":
		return fmt.Sprintf("tok_%s", m.hmacHex(value)[:16])
	default:
		// Fall back to automatic heuristic matching.
		return m.MaskColumn(columnName, value)
	}
}

// Redact returns the fixed placeholder "[REDACTED]" regardless of input.
func (m *Masker) Redact(_ string) string {
	return "[REDACTED]"
}

// UUIDRemap returns a deterministic UUID v4 derived from the HMAC of value.
// The UUID is RFC 4122 compliant: version bits (4) and variant bits (10) are set.
func (m *Masker) UUIDRemap(value string) string {
	b := m.hmacBytes(value) // 32 bytes; we use the first 16.
	// Set version 4 (0100xxxx in byte 6).
	b[6] = (b[6] & 0x0f) | 0x40
	// Set variant 10 (10xxxxxx in byte 8).
	b[8] = (b[8] & 0x3f) | 0x80

	// Format as 8-4-4-4-12.
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:16],
	)
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
