package graph

import (
	"fmt"
	"sort"
	"strings"
)

// KeysetItem represents a composite entity identifier (table_name, primary_key) in a BFS traversal queue.
type KeysetItem struct {
	Table string
	PK    string
}

// KeysetCursor points to the last processed (table_name, primary_key) tuple.
type KeysetCursor struct {
	Table string
	PK    string
}

// CompareKeyset compares two keyset items: returns -1 if a < b, 0 if a == b, +1 if a > b.
func CompareKeyset(aTable, aPK, bTable, bPK string) int {
	if aTable != bTable {
		if aTable < bTable {
			return -1
		}
		return 1
	}
	if aPK != bPK {
		if aPK < bPK {
			return -1
		}
		return 1
	}
	return 0
}

// KeysetQueue manages an ordered queue of composite keys (table_name, pk)
// and provides O(1) cursor seek pagination without OFFSET degradation.
type KeysetQueue struct {
	items   []KeysetItem
	dedup   map[string]map[string]bool
	isDirty bool
}

// NewKeysetQueue creates an empty KeysetQueue.
func NewKeysetQueue() *KeysetQueue {
	return &KeysetQueue{
		items: make([]KeysetItem, 0),
		dedup: make(map[string]map[string]bool),
	}
}

// Enqueue adds an entity (table, pk) to the queue if not already present.
func (q *KeysetQueue) Enqueue(table, pk string) bool {
	if q.dedup[table] == nil {
		q.dedup[table] = make(map[string]bool)
	}
	if q.dedup[table][pk] {
		return false
	}
	q.dedup[table][pk] = true
	q.items = append(q.items, KeysetItem{Table: table, PK: pk})
	q.isDirty = true
	return true
}

// Len returns the total number of items in the queue.
func (q *KeysetQueue) Len() int {
	return len(q.items)
}

func (q *KeysetQueue) ensureSorted() {
	if !q.isDirty {
		return
	}
	sort.Slice(q.items, func(i, j int) bool {
		return CompareKeyset(q.items[i].Table, q.items[i].PK, q.items[j].Table, q.items[j].PK) < 0
	})
	q.isDirty = false
}

// SeekPage retrieves the next page of items strictly after cursor using binary search seek.
// Seek time is O(log N) memory seek, completely eliminating O(N) OFFSET scan degradation.
func (q *KeysetQueue) SeekPage(cursor *KeysetCursor, batchSize int) (items []KeysetItem, nextCursor *KeysetCursor, hasMore bool) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	q.ensureSorted()

	if len(q.items) == 0 {
		return nil, nil, false
	}

	startIdx := 0
	if cursor != nil {
		// Binary search for first item > cursor
		startIdx = sort.Search(len(q.items), func(i int) bool {
			return CompareKeyset(q.items[i].Table, q.items[i].PK, cursor.Table, cursor.PK) > 0
		})
	}

	if startIdx >= len(q.items) {
		return nil, nil, false
	}

	endIdx := startIdx + batchSize
	if endIdx > len(q.items) {
		endIdx = len(q.items)
	}

	page := q.items[startIdx:endIdx]
	last := page[len(page)-1]
	newCursor := &KeysetCursor{Table: last.Table, PK: last.PK}
	more := endIdx < len(q.items)

	return page, newCursor, more
}

// Paginate iterates over all items in batches using SeekPage until all items are consumed or fn returns an error.
func (q *KeysetQueue) Paginate(batchSize int, fn func([]KeysetItem) error) error {
	var cursor *KeysetCursor
	for {
		page, nextCursor, hasMore := q.SeekPage(cursor, batchSize)
		if len(page) == 0 {
			break
		}
		if err := fn(page); err != nil {
			return err
		}
		if !hasMore {
			break
		}
		cursor = nextCursor
	}
	return nil
}

// BuildKeysetSeekQuery constructs a cursor seek pagination query for batch traversal lookups.
// For PostgreSQL:
//   First page:       SELECT colNames FROM schema.table ORDER BY table_col, pk_col LIMIT :batch_size
//   Subsequent pages: SELECT colNames FROM schema.table WHERE (table_col, pk_col) > ($1, $2) ORDER BY table_col, pk_col LIMIT :batch_size
// For MySQL:
//   First page:       SELECT colNames FROM `schema`.`table` ORDER BY `table_col`, `pk_col` LIMIT :batch_size
//   Subsequent pages: SELECT colNames FROM `schema`.`table` WHERE (`table_col`, `pk_col`) > (?, ?) ORDER BY `table_col`, `pk_col` LIMIT :batch_size
func BuildKeysetSeekQuery(
	engine string,
	schema, table string,
	colNames []string,
	tableCol, pkCol string,
	batchSize int,
	hasCursor bool,
) string {
	isMySQL := strings.ToLower(engine) == "mysql"

	quote := func(ident string) string {
		if isMySQL {
			return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
		}
		return QuoteIdent(ident)
	}

	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = quote(c)
	}

	qualifiedTable := quote(table)
	if schema != "" {
		if isMySQL {
			qualifiedTable = quote(schema) + "." + quote(table)
		} else {
			qualifiedTable = quote(schema) + "." + quote(table)
		}
	}

	qTableCol := quote(tableCol)
	qPkCol := quote(pkCol)

	var whereClause string
	if hasCursor {
		if isMySQL {
			whereClause = fmt.Sprintf(" WHERE (%s, %s) > (?, ?)", qTableCol, qPkCol)
		} else {
			whereClause = fmt.Sprintf(" WHERE (%s, %s) > ($1, $2)", qTableCol, qPkCol)
		}
	}

	return fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s, %s LIMIT %d",
		strings.Join(quotedCols, ", "),
		qualifiedTable,
		whereClause,
		qTableCol,
		qPkCol,
		batchSize,
	)
}
