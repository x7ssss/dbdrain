package drain

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/x7ssss/dbdrain/internal/db"
	"github.com/x7ssss/dbdrain/internal/introspect"
)

// FormatValue formats a single database value into a SQL literal according to engine rules.
func FormatValue(engine db.EngineType, col introspect.Column, v any) string {
	if v == nil {
		return "NULL"
	}

	if valuer, ok := v.(driver.Valuer); ok {
		val, err := valuer.Value()
		if err != nil || val == nil {
			return "NULL"
		}
		dt := strings.ToLower(col.DataType)
		if strings.Contains(dt, "numeric") || strings.Contains(dt, "decimal") {
			return fmt.Sprintf("%v", val)
		}
		return FormatValue(engine, col, val)
	}

	switch val := v.(type) {
	case bool:
		if engine == db.EngineMySQL || engine == db.EngineSQLite {
			if val {
				return "1"
			}
			return "0"
		}
		if val {
			return "TRUE"
		}
		return "FALSE"

	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprintf("%v", val)

	case []byte:
		dt := strings.ToLower(col.DataType)
		if strings.Contains(dt, "json") {
			if engine == db.EngineMySQL {
				return "'" + escapeMySQLString(string(val)) + "'"
			}
			return "'" + escapePostgresString(string(val)) + "'"
		}
		// Text/varchar-like types that the driver returned as []byte
		if strings.Contains(dt, "char") || strings.Contains(dt, "text") || strings.Contains(dt, "enum") || strings.Contains(dt, "set") {
			if engine == db.EngineMySQL {
				return "'" + escapeMySQLString(string(val)) + "'"
			}
			return "'" + escapePostgresString(string(val)) + "'"
		}
		// Binary types
		if engine == db.EngineMySQL || engine == db.EngineSQLite {
			return fmt.Sprintf("X'%X'", val)
		}
		return fmt.Sprintf("'\\x%x'", val)

	case time.Time:
		if engine == db.EngineMySQL || engine == db.EngineSQLite {
			if val.Nanosecond() == 0 {
				return fmt.Sprintf("'%s'", val.UTC().Format("2006-01-02 15:04:05"))
			}
			return fmt.Sprintf("'%s'", val.UTC().Format("2006-01-02 15:04:05.000000"))
		}
		return fmt.Sprintf("'%s'", val.UTC().Format(time.RFC3339Nano))

	case [16]byte:
		if engine == db.EngineMySQL {
			return "'" + escapeMySQLString(formatUUID(val)) + "'"
		}
		return fmt.Sprintf("'%s'", formatUUID(val))

	default:
		s := fmt.Sprintf("%v", val)
		if engine == db.EngineMySQL {
			return "'" + escapeMySQLString(s) + "'"
		}
		return "'" + escapePostgresString(s) + "'"
	}
}

// FormatValues formats a row of column values into a comma-separated SQL VALUES list.
func FormatValues(engine db.EngineType, cols []introspect.Column, vals []any) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		var col introspect.Column
		if i < len(cols) {
			col = cols[i]
		}
		parts[i] = FormatValue(engine, col, v)
	}
	return strings.Join(parts, ", ")
}

// escapePostgresString escapes single quotes for PostgreSQL string literals.
func escapePostgresString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// escapeMySQLString escapes characters for standard MySQL/MariaDB string literals.
func escapeMySQLString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		switch r {
		case 0:
			b.WriteString(`\0`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\\':
			b.WriteString(`\\`)
		case '\'':
			b.WriteString(`\'`)
		case '"':
			b.WriteString(`\"`)
		case '\032': // 0x1A / Ctrl+Z
			b.WriteString(`\Z`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
