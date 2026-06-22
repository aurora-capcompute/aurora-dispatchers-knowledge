package knowledge

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	root := t.TempDir()
	settings := Settings{
		DatabasePath: filepath.Join(t.TempDir(), "knowledge.db"),
		Roots:        []string{root}, Collections: []string{"default", "work"},
		Extensions: []string{".md", ".txt"}, AllowWrite: true, AllowDelete: true,
		MaxDocumentBytes: 1 << 20, MaxChunkBytes: 80, ChunkOverlapBytes: 10,
		MaxResults: 10, MaxBatch: 10, EmbeddingTimeoutMS: 1000,
	}
	store, err := openStore(context.Background(), settings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.db.Close() })
	return store
}

func TestPutSearchGetDelete(t *testing.T) {
	store := testStore(t)
	source, err := store.Put(context.Background(), PutRequest{
		ID: "note-1", Title: "Aurora", Text: "Durable capability dispatch and replay.",
	})
	if err != nil {
		t.Fatal(err)
	}
	results, err := store.Search(context.Background(), SearchRequest{Query: "capability replay"})
	if err != nil || len(results) == 0 || results[0].SourceID != source.ID || results[0].Citation == "" {
		t.Fatalf("results=%#v err=%v", results, err)
	}
	got, err := store.Get(context.Background(), source.ID)
	if err != nil || got.Text != source.Text {
		t.Fatalf("got=%#v err=%v", got, err)
	}
	deleted, err := store.Delete(context.Background(), source.ID)
	if err != nil || deleted["deleted"] != true {
		t.Fatalf("deleted=%#v err=%v", deleted, err)
	}
	if _, err := store.Get(context.Background(), source.ID); err == nil {
		t.Fatal("deleted source still exists")
	}
}

func TestIngestIsIdempotentByPath(t *testing.T) {
	store := testStore(t)
	root := store.settings.Roots[0]
	path := filepath.Join(root, "doc.md")
	if err := os.WriteFile(path, []byte("# One\nfirst"), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := store.Ingest(context.Background(), IngestRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("# Two\nsecond"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := store.Ingest(context.Background(), IngestRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("source id changed: %s != %s", first.ID, second.ID)
	}
	got, _ := store.Get(context.Background(), first.ID)
	if !strings.Contains(got.Text, "second") {
		t.Fatalf("source was not replaced: %#v", got)
	}
}

func TestPersistenceAcrossOpen(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(t.TempDir(), "knowledge.db")
	settings := Settings{
		DatabasePath: path, Roots: []string{root}, Collections: []string{"default"},
		Extensions: []string{".txt"}, AllowWrite: true, MaxDocumentBytes: 1000,
		MaxChunkBytes: 100, MaxResults: 10, MaxBatch: 10, EmbeddingTimeoutMS: 1000,
	}
	first, err := openStore(context.Background(), settings)
	if err != nil {
		t.Fatal(err)
	}
	_, err = first.Put(context.Background(), PutRequest{ID: "persistent", Title: "P", Text: "survives restart"})
	if err != nil {
		t.Fatal(err)
	}
	first.db.Close()
	second, err := openStore(context.Background(), settings)
	if err != nil {
		t.Fatal(err)
	}
	defer second.db.Close()
	source, err := second.Get(context.Background(), "persistent")
	if err != nil || source.Text != "survives restart" {
		t.Fatalf("source=%#v err=%v", source, err)
	}
}

func TestSchemasAreJSON(t *testing.T) {
	settings := Settings{Collections: []string{"default"}}
	for _, name := range capabilityNames {
		if !json.Valid(capability(name, settings).InputSchema) {
			t.Fatalf("invalid schema for %s", name)
		}
	}
}
