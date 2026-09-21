package graph

import (
	"testing"

	"github.com/x7ssss/dbdrain/internal/introspect"
)

func makeBasicSchema(tables []string) *introspect.Schema {
	s := &introspect.Schema{
		Columns:     make(map[string][]introspect.Column),
		PrimaryKeys: make(map[string][]string),
	}
	for _, t := range tables {
		s.Columns[t] = []introspect.Column{{Name: "id", DataType: "int", Ordinal: 1}}
		s.PrimaryKeys[t] = []string{"id"}
	}
	return s
}

// TestVirtualEdgeRegistration verifies virtual FKs are added to VirtualEdges map.
func TestVirtualEdgeRegistration(t *testing.T) {
	schema := makeBasicSchema([]string{"comments", "posts", "videos"})
	vfks := []VirtualFKInput{
		{
			ChildTable:       "comments",
			ChildColumn:      "commentable_id",
			DiscriminatorCol: "commentable_type",
			Mappings: map[string]string{
				"post":  "posts.id",
				"video": "videos.id",
			},
		},
	}

	g := New(schema, vfks)

	edges, ok := g.VirtualEdges["comments"]
	if !ok || len(edges) != 1 {
		t.Fatalf("expected 1 virtual edge for 'comments', got %d", len(edges))
	}
	ve := edges[0]
	if ve.ChildColumn != "commentable_id" {
		t.Errorf("expected ChildColumn=commentable_id, got %s", ve.ChildColumn)
	}
	if ve.DiscriminatorCol != "commentable_type" {
		t.Errorf("expected DiscriminatorCol=commentable_type, got %s", ve.DiscriminatorCol)
	}
}

// TestVirtualEdgeMappingParsed verifies that "posts.id" is parsed into {posts, id}.
func TestVirtualEdgeMappingParsed(t *testing.T) {
	schema := makeBasicSchema([]string{"comments", "posts"})
	vfks := []VirtualFKInput{
		{
			ChildTable:       "comments",
			ChildColumn:      "commentable_id",
			DiscriminatorCol: "commentable_type",
			Mappings:         map[string]string{"post": "posts.id"},
		},
	}

	g := New(schema, vfks)
	ve := g.VirtualEdges["comments"][0]
	tc, ok := ve.Mappings["post"]
	if !ok {
		t.Fatal("expected 'post' mapping to exist")
	}
	if tc.Table != "posts" || tc.Column != "id" {
		t.Errorf("expected {posts, id}, got {%s, %s}", tc.Table, tc.Column)
	}
}

// TestVirtualParentNodesRegistered verifies parent tables from virtual FKs are
// added as nodes even if not in schema.Columns.
func TestVirtualParentNodesRegistered(t *testing.T) {
	// Only 'comments' is in the schema; 'posts' and 'videos' are not.
	schema := makeBasicSchema([]string{"comments"})
	vfks := []VirtualFKInput{
		{
			ChildTable:       "comments",
			ChildColumn:      "commentable_id",
			DiscriminatorCol: "commentable_type",
			Mappings:         map[string]string{"post": "posts.id", "video": "videos.id"},
		},
	}

	g := New(schema, vfks)
	if _, ok := g.Nodes["posts"]; !ok {
		t.Error("expected 'posts' node to be registered from virtual FK mapping")
	}
	if _, ok := g.Nodes["videos"]; !ok {
		t.Error("expected 'videos' node to be registered from virtual FK mapping")
	}
}

// TestNoVirtualEdgesWhenNoneProvided verifies the VirtualEdges map is empty
// when New is called without vfks (backward-compat with v0.2.0 call sites).
func TestNoVirtualEdgesWhenNoneProvided(t *testing.T) {
	schema := makeBasicSchema([]string{"users", "orders"})
	schema.ForeignKeys = []introspect.ForeignKey{
		{FromTable: "orders", FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
	}
	g := New(schema) // no virtual FKs
	if len(g.VirtualEdges) != 0 {
		t.Errorf("expected empty VirtualEdges, got %v", g.VirtualEdges)
	}
}

// TestMultipleVirtualEdgesSameTable verifies multiple virtual FK edges on one table.
func TestMultipleVirtualEdgesSameTable(t *testing.T) {
	schema := makeBasicSchema([]string{"attachments", "posts", "users"})
	vfks := []VirtualFKInput{
		{ChildTable: "attachments", ChildColumn: "attachable_id", DiscriminatorCol: "attachable_type",
			Mappings: map[string]string{"post": "posts.id"}},
		{ChildTable: "attachments", ChildColumn: "owner_id", DiscriminatorCol: "owner_type",
			Mappings: map[string]string{"user": "users.id"}},
	}
	g := New(schema, vfks)
	if len(g.VirtualEdges["attachments"]) != 2 {
		t.Errorf("expected 2 virtual edges for 'attachments', got %d", len(g.VirtualEdges["attachments"]))
	}
}
