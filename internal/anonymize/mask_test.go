package anonymize

import (
	"strings"
	"testing"
)

func TestMaskEmail(t *testing.T) {
	m := New("test-salt")
	result := m.MaskColumn("email", "user@example.com")
	if !strings.HasSuffix(result, "@drain.local") {
		t.Errorf("email mask should end with @drain.local, got: %s", result)
	}
	if !strings.HasPrefix(result, "anon_") {
		t.Errorf("email mask should start with anon_, got: %s", result)
	}
}

func TestMaskEmailDeterministic(t *testing.T) {
	m := New("test-salt")
	a := m.MaskColumn("email", "user@example.com")
	b := m.MaskColumn("email", "user@example.com")
	if a != b {
		t.Errorf("masking should be deterministic: %s != %s", a, b)
	}
}

func TestMaskEmailDistinct(t *testing.T) {
	m := New("test-salt")
	a := m.MaskColumn("email", "user1@example.com")
	b := m.MaskColumn("email", "user2@example.com")
	if a == b {
		t.Errorf("different inputs should produce different masked values")
	}
}

func TestMaskPhone(t *testing.T) {
	m := New("test-salt")
	result := m.MaskColumn("phone", "555-1234")
	if !strings.HasPrefix(result, "+1555") {
		t.Errorf("phone mask should start with +1555, got: %s", result)
	}
}

func TestMaskFirstName(t *testing.T) {
	m := New("test-salt")
	result := m.MaskColumn("first_name", "Alice")
	if !strings.HasPrefix(result, "User_") {
		t.Errorf("first_name mask should start with User_, got: %s", result)
	}
}

func TestMaskLastName(t *testing.T) {
	m := New("test-salt")
	result := m.MaskColumn("last_name", "Smith")
	if !strings.HasPrefix(result, "Anon_") {
		t.Errorf("last_name mask should start with Anon_, got: %s", result)
	}
}

func TestMaskPassword(t *testing.T) {
	m := New("test-salt")
	result := m.MaskColumn("password", "secret123")
	if !strings.HasPrefix(result, "$2a$12$") {
		t.Errorf("password mask should be bcrypt placeholder, got: %s", result)
	}
}

func TestMaskToken(t *testing.T) {
	m := New("test-salt")
	result := m.MaskColumn("token", "abc123xyz")
	if !strings.HasPrefix(result, "tok_") {
		t.Errorf("token mask should start with tok_, got: %s", result)
	}
}

func TestNoMask(t *testing.T) {
	m := New("test-salt")
	result := m.MaskColumn("created_at", "2024-01-01")
	if result != "2024-01-01" {
		t.Errorf("non-PII column should not be masked, got: %s", result)
	}
}

func TestSaltIsolation(t *testing.T) {
	m1 := New("salt-one")
	m2 := New("salt-two")
	a := m1.MaskColumn("email", "user@example.com")
	b := m2.MaskColumn("email", "user@example.com")
	if a == b {
		t.Errorf("different salts should produce different masked values")
	}
}
