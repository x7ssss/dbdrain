package anonymize

import (
	"strings"
	"testing"
)

// Tests for new v0.3.0 transforms.

func TestRedact(t *testing.T) {
	m := New("test-salt")
	result := m.Redact("any value here")
	if result != "[REDACTED]" {
		t.Errorf("expected [REDACTED], got %s", result)
	}
}

func TestRedactDeterministic(t *testing.T) {
	m := New("test-salt")
	a := m.Redact("value1")
	b := m.Redact("value2")
	if a != b || a != "[REDACTED]" {
		t.Errorf("Redact should always return [REDACTED], got a=%s b=%s", a, b)
	}
}

func TestUUIDRemap(t *testing.T) {
	m := New("test-salt")
	result := m.UUIDRemap("user@example.com")
	// UUID format: 8-4-4-4-12 hex chars
	parts := strings.Split(result, "-")
	if len(parts) != 5 {
		t.Fatalf("expected UUID with 5 parts, got %d: %s", len(parts), result)
	}
	if len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 {
		t.Errorf("UUID segment lengths wrong: %s", result)
	}
}

func TestUUIDRemapVersion4(t *testing.T) {
	m := New("test-salt")
	result := m.UUIDRemap("hello")
	// Version bit: 3rd group starts with '4'
	parts := strings.Split(result, "-")
	if !strings.HasPrefix(parts[2], "4") {
		t.Errorf("UUID version should be 4, got third group: %s", parts[2])
	}
}

func TestUUIDRemapVariant(t *testing.T) {
	m := New("test-salt")
	result := m.UUIDRemap("hello")
	parts := strings.Split(result, "-")
	// Variant bits: 4th group first hex nibble should be 8, 9, a, or b
	firstNibble := parts[3][0]
	if firstNibble != '8' && firstNibble != '9' && firstNibble != 'a' && firstNibble != 'b' {
		t.Errorf("UUID variant should be 8/9/a/b, got: %c (group: %s)", firstNibble, parts[3])
	}
}

func TestUUIDRemapDeterministic(t *testing.T) {
	m := New("test-salt")
	a := m.UUIDRemap("user@example.com")
	b := m.UUIDRemap("user@example.com")
	if a != b {
		t.Errorf("UUIDRemap should be deterministic: %s != %s", a, b)
	}
}

func TestUUIDRemapDistinct(t *testing.T) {
	m := New("test-salt")
	a := m.UUIDRemap("value1")
	b := m.UUIDRemap("value2")
	if a == b {
		t.Error("different inputs should produce different UUIDs")
	}
}

func TestApplyTransformFakeEmail(t *testing.T) {
	m := New("test-salt")
	result := m.ApplyTransform("fake_email", "email", "user@example.com")
	if !strings.HasSuffix(result, "@drain.local") || !strings.HasPrefix(result, "anon_") {
		t.Errorf("fake_email transform wrong: %s", result)
	}
}

func TestApplyTransformRedact(t *testing.T) {
	m := New("test-salt")
	result := m.ApplyTransform("redact", "password", "secret123")
	if result != "[REDACTED]" {
		t.Errorf("redact transform wrong: %s", result)
	}
}

func TestApplyTransformUUIDRemap(t *testing.T) {
	m := New("test-salt")
	result := m.ApplyTransform("uuid_remap", "access_token", "tok_abc123")
	if len(strings.Split(result, "-")) != 5 {
		t.Errorf("uuid_remap should produce UUID, got: %s", result)
	}
}

func TestApplyTransformHMACPhone(t *testing.T) {
	m := New("test-salt")
	result := m.ApplyTransform("hmac_phone", "phone", "555-1234")
	if !strings.HasPrefix(result, "+1555") {
		t.Errorf("hmac_phone transform wrong: %s", result)
	}
}

func TestApplyTransformFallback(t *testing.T) {
	m := New("test-salt")
	// Unknown transform → falls back to auto-heuristic (email col → anon_ format)
	result := m.ApplyTransform("unknown_transform", "email", "user@example.com")
	if !strings.HasSuffix(result, "@drain.local") {
		t.Errorf("fallback should apply auto heuristic for email, got: %s", result)
	}
}

func TestApplyTransformNoopForUnrecognized(t *testing.T) {
	m := New("test-salt")
	// Unknown transform + non-PII column → returns original value
	result := m.ApplyTransform("unknown_transform", "created_at", "2024-01-01")
	if result != "2024-01-01" {
		t.Errorf("unrecognized transform on non-PII col should be identity, got: %s", result)
	}
}
