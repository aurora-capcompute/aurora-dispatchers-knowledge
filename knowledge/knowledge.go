package knowledge

import (
	"github.com/aurora-capcompute/aurora-dispatchers-documents/documents"
	"github.com/aurora-capcompute/aurora-dispatchers/builtin"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/aurora-dispatchers/resolution"
	"bytes"
	"github.com/aurora-capcompute/capcompute/dispatcher"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	Ingest  = "knowledge.ingest"
	Put     = "knowledge.put"
	Search  = "knowledge.search"
	Get     = "knowledge.get"
	Sources = "knowledge.sources"
	Delete  = "knowledge.delete"
	Reindex = "knowledge.reindex"
)

var capabilityNames = []string{Ingest, Put, Search, Get, Sources, Delete, Reindex}

type Settings struct {
	DatabasePath       string   `json:"database_path"`
	Roots              []string `json:"roots,omitempty"`
	Collections        []string `json:"collections,omitempty"`
	Extensions         []string `json:"extensions,omitempty"`
	AllowWrite         bool     `json:"allow_write,omitempty"`
	AllowDelete        bool     `json:"allow_delete,omitempty"`
	RequireApproval    bool     `json:"require_approval,omitempty"`
	MaxDocumentBytes   int64    `json:"max_document_bytes,omitempty"`
	MaxChunkBytes      int      `json:"max_chunk_bytes,omitempty"`
	ChunkOverlapBytes  int      `json:"chunk_overlap_bytes,omitempty"`
	MaxResults         int      `json:"max_results,omitempty"`
	MaxBatch           int      `json:"max_batch,omitempty"`
	PDFTextBinary      string   `json:"pdf_text_binary,omitempty"`
	EmbeddingBaseURL   string   `json:"embedding_base_url,omitempty"`
	EmbeddingModel     string   `json:"embedding_model,omitempty"`
	EmbeddingAPIKeyEnv string   `json:"embedding_api_key_env,omitempty"`
	EmbeddingTimeoutMS int64    `json:"embedding_timeout_ms,omitempty"`
}

type Registration struct{}

func (Registration) Matches(name string) bool { return slices.Contains(capabilityNames, name) }

func (Registration) Normalize(name string, raw json.RawMessage) (json.RawMessage, error) {
	if !slices.Contains(capabilityNames, name) {
		return nil, fmt.Errorf("unsupported knowledge capability %q", name)
	}
	settings := Settings{
		Collections:        []string{"default"},
		Extensions:         []string{".go", ".html", ".htm", ".json", ".md", ".pdf", ".txt", ".yaml", ".yml"},
		MaxDocumentBytes:   20 << 20,
		MaxChunkBytes:      4000,
		ChunkOverlapBytes:  400,
		MaxResults:         20,
		MaxBatch:           50,
		PDFTextBinary:      "pdftotext",
		EmbeddingTimeoutMS: int64((30 * time.Second) / time.Millisecond),
	}
	if name == Delete || name == Reindex {
		settings.RequireApproval = true
	}
	if err := decodeStrict(raw, &settings); err != nil {
		return nil, err
	}
	if strings.TrimSpace(settings.DatabasePath) == "" {
		return nil, errors.New("database_path is required")
	}
	dbPath, err := filepath.Abs(settings.DatabasePath)
	if err != nil {
		return nil, err
	}
	settings.DatabasePath = filepath.Clean(dbPath)
	settings.Roots, err = canonicalRoots(settings.Roots)
	if err != nil {
		return nil, err
	}
	settings.Collections, err = identifiers(settings.Collections, "collection")
	if err != nil {
		return nil, err
	}
	settings.Extensions, err = extensions(settings.Extensions)
	if err != nil {
		return nil, err
	}
	if settings.MaxDocumentBytes <= 0 || settings.MaxChunkBytes <= 0 || settings.MaxResults <= 0 ||
		settings.MaxBatch <= 0 || settings.EmbeddingTimeoutMS <= 0 {
		return nil, errors.New("knowledge limits must be positive")
	}
	if settings.ChunkOverlapBytes < 0 || settings.ChunkOverlapBytes >= settings.MaxChunkBytes {
		return nil, errors.New("chunk_overlap_bytes must be non-negative and smaller than max_chunk_bytes")
	}
	if isWrite(name) && !settings.AllowWrite {
		return nil, fmt.Errorf("%s requires allow_write=true", name)
	}
	if name == Delete && !settings.AllowDelete {
		return nil, errors.New("knowledge.delete requires allow_delete=true")
	}
	if name == Ingest && len(settings.Roots) == 0 {
		return nil, errors.New("knowledge.ingest requires at least one root")
	}
	if settings.EmbeddingBaseURL != "" {
		if strings.TrimSpace(settings.EmbeddingModel) == "" {
			return nil, errors.New("embedding_model is required with embedding_base_url")
		}
		normalized, err := validateBaseURL(settings.EmbeddingBaseURL)
		if err != nil {
			return nil, err
		}
		settings.EmbeddingBaseURL = normalized
		if settings.EmbeddingAPIKeyEnv == "" {
			settings.EmbeddingAPIKeyEnv = "OPENAI_API_KEY"
		}
	}
	return json.Marshal(settings)
}

func (Registration) IsSubset(_ string, parent, child json.RawMessage) error {
	var p, c Settings
	if err := json.Unmarshal(parent, &p); err != nil {
		return fmt.Errorf("decode parent settings: %w", err)
	}
	if err := json.Unmarshal(child, &c); err != nil {
		return fmt.Errorf("decode child settings: %w", err)
	}
	if p.DatabasePath != c.DatabasePath || p.EmbeddingBaseURL != c.EmbeddingBaseURL ||
		p.EmbeddingModel != c.EmbeddingModel || p.EmbeddingAPIKeyEnv != c.EmbeddingAPIKeyEnv {
		return errors.New("child knowledge store or embedding provider differs from parent")
	}
	for _, root := range c.Roots {
		if !insideAny(root, p.Roots) {
			return fmt.Errorf("child root %q is outside parent roots", root)
		}
	}
	for _, collection := range c.Collections {
		if !slices.Contains(p.Collections, collection) {
			return fmt.Errorf("child collection %q is not allowed by parent", collection)
		}
	}
	for _, extension := range c.Extensions {
		if !slices.Contains(p.Extensions, extension) {
			return fmt.Errorf("child extension %q is not allowed by parent", extension)
		}
	}
	if !p.AllowWrite && c.AllowWrite || !p.AllowDelete && c.AllowDelete || p.RequireApproval && !c.RequireApproval {
		return errors.New("child knowledge policy widens parent permissions")
	}
	if c.MaxDocumentBytes > p.MaxDocumentBytes || c.MaxChunkBytes > p.MaxChunkBytes ||
		c.ChunkOverlapBytes > p.ChunkOverlapBytes || c.MaxResults > p.MaxResults ||
		c.MaxBatch > p.MaxBatch || c.EmbeddingTimeoutMS > p.EmbeddingTimeoutMS {
		return errors.New("child knowledge limits exceed parent limits")
	}
	return nil
}

func (Registration) Configure(ctx context.Context, name string, raw json.RawMessage, _ registry.Services, config *builtin.Config) error {
	normalized, err := (Registration{}).Normalize(name, raw)
	if err != nil {
		return err
	}
	var settings Settings
	if err := json.Unmarshal(normalized, &settings); err != nil {
		return err
	}
	store, err := openStore(ctx, settings)
	if err != nil {
		return err
	}
	config.Handlers = append(config.Handlers, &Handler{name: name, settings: settings, store: store})
	config.Capabilities = append(config.Capabilities, capability(name, settings))
	return nil
}

type Handler struct {
	name     string
	settings Settings
	store    *Store
}

func (h *Handler) Handles(name string) bool { return name == h.name }

func (h *Handler) DispatchCall(ctx context.Context, call dispatcher.Call) (dispatcher.Outcome, error) {
	if h.settings.RequireApproval {
		if resolved, ok := resolution.FromContext(ctx); !ok || resolved.Decision != resolution.Approved {
			return dispatcher.Yield("Approve " + call.Name + " in knowledge store " + h.settings.DatabasePath), nil
		}
	}
	var result any
	var err error
	switch call.Name {
	case Ingest:
		var req IngestRequest
		err = decodeStrict(call.Args, &req)
		if err == nil {
			result, err = h.store.Ingest(ctx, req)
		}
	case Put:
		var req PutRequest
		err = decodeStrict(call.Args, &req)
		if err == nil {
			result, err = h.store.Put(ctx, req)
		}
	case Search:
		var req SearchRequest
		err = decodeStrict(call.Args, &req)
		if err == nil {
			result, err = h.store.Search(ctx, req)
		}
	case Get:
		var req IDRequest
		err = decodeStrict(call.Args, &req)
		if err == nil {
			result, err = h.store.Get(ctx, req.ID)
		}
	case Sources:
		var req SourcesRequest
		err = decodeStrict(call.Args, &req)
		if err == nil {
			result, err = h.store.Sources(ctx, req)
		}
	case Delete:
		var req IDRequest
		err = decodeStrict(call.Args, &req)
		if err == nil {
			result, err = h.store.Delete(ctx, req.ID)
		}
	case Reindex:
		var req SourcesRequest
		err = decodeStrict(call.Args, &req)
		if err == nil {
			result, err = h.store.Reindex(ctx, req.Collection)
		}
	default:
		return dispatcher.Failed("unknown knowledge call: " + call.Name), nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return dispatcher.Outcome{}, ctx.Err()
		}
		return dispatcher.Failed(err.Error()), nil
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return dispatcher.Outcome{}, err
	}
	return dispatcher.Result(raw), nil
}

type IngestRequest struct {
	Path       string `json:"path"`
	Collection string `json:"collection,omitempty"`
}

type PutRequest struct {
	ID         string `json:"id,omitempty"`
	Title      string `json:"title"`
	Text       string `json:"text"`
	Collection string `json:"collection,omitempty"`
}

type SearchRequest struct {
	Query      string `json:"query"`
	Collection string `json:"collection,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

type SourcesRequest struct {
	Collection string `json:"collection,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

type IDRequest struct {
	ID string `json:"id"`
}

type Source struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	Path       string    `json:"path,omitempty"`
	Title      string    `json:"title"`
	Collection string    `json:"collection"`
	Hash       string    `json:"hash"`
	MediaType  string    `json:"media_type"`
	Text       string    `json:"text,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type SearchResult struct {
	SourceID   string  `json:"source_id"`
	Path       string  `json:"path,omitempty"`
	Title      string  `json:"title"`
	Collection string  `json:"collection"`
	Position   int     `json:"position"`
	Text       string  `json:"text"`
	Score      float64 `json:"score"`
	Citation   string  `json:"citation"`
}

type Store struct {
	db        *sql.DB
	settings  Settings
	extractor documents.Extractor
	embedder  *embedder
}

func openStore(ctx context.Context, settings Settings) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(settings.DatabasePath), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", settings.DatabasePath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;`); err != nil {
		db.Close()
		return nil, err
	}
	schema := `
CREATE TABLE IF NOT EXISTS sources (
	id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	path TEXT NOT NULL DEFAULT '',
	title TEXT NOT NULL,
	collection TEXT NOT NULL,
	hash TEXT NOT NULL,
	media_type TEXT NOT NULL,
	text TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS sources_path_collection
	ON sources(path, collection) WHERE path <> '';
CREATE TABLE IF NOT EXISTS chunks (
	source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
	position INTEGER NOT NULL,
	text TEXT NOT NULL,
	PRIMARY KEY(source_id, position)
);
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
	source_id UNINDEXED,
	position UNINDEXED,
	text,
	tokenize='unicode61'
);
CREATE TABLE IF NOT EXISTS embeddings (
	source_id TEXT NOT NULL,
	position INTEGER NOT NULL,
	vector TEXT NOT NULL,
	PRIMARY KEY(source_id, position),
	FOREIGN KEY(source_id, position) REFERENCES chunks(source_id, position) ON DELETE CASCADE
);`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize knowledge database: %w", err)
	}
	docSettings := documents.Settings{
		Roots: settings.Roots, Extensions: settings.Extensions,
		MaxSourceBytes: settings.MaxDocumentBytes, MaxExtractedBytes: settings.MaxDocumentBytes,
		MaxPDFPages: 200, MaxBatch: settings.MaxBatch, PDFTextBinary: settings.PDFTextBinary,
		TimeoutMS: settings.EmbeddingTimeoutMS,
	}
	store := &Store{db: db, settings: settings, extractor: documents.NewExtractor(docSettings)}
	if settings.EmbeddingBaseURL != "" {
		store.embedder = &embedder{
			baseURL: settings.EmbeddingBaseURL, model: settings.EmbeddingModel,
			apiKeyEnv: settings.EmbeddingAPIKeyEnv,
			client:    &http.Client{Timeout: time.Duration(settings.EmbeddingTimeoutMS) * time.Millisecond},
		}
	}
	return store, nil
}

func (s *Store) Ingest(ctx context.Context, req IngestRequest) (Source, error) {
	collection, err := s.collection(req.Collection)
	if err != nil {
		return Source{}, err
	}
	document, err := s.extractor.Extract(ctx, req.Path)
	if err != nil {
		return Source{}, err
	}
	id := "file_" + hashString(collection + "\x00" + document.SourcePath)[:24]
	source := Source{
		ID: id, Kind: "file", Path: document.SourcePath, Title: document.Title,
		Collection: collection, Hash: document.Hash, MediaType: document.MediaType, Text: document.Text,
	}
	return source, s.replace(ctx, source)
}

func (s *Store) Put(ctx context.Context, req PutRequest) (Source, error) {
	collection, err := s.collection(req.Collection)
	if err != nil {
		return Source{}, err
	}
	if strings.TrimSpace(req.Title) == "" || strings.TrimSpace(req.Text) == "" {
		return Source{}, errors.New("title and text are required")
	}
	if int64(len(req.Text)) > s.settings.MaxDocumentBytes {
		return Source{}, errors.New("text exceeds max_document_bytes")
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = "note_" + hashString(collection + "\x00" + req.Title + "\x00" + req.Text)[:24]
	}
	if !safeID(id) {
		return Source{}, errors.New("invalid note id")
	}
	source := Source{
		ID: id, Kind: "note", Title: req.Title, Collection: collection,
		Hash: hashString(req.Text), MediaType: "text/markdown", Text: req.Text,
	}
	return source, s.replace(ctx, source)
}

func (s *Store) replace(ctx context.Context, source Source) error {
	chunks := chunkText(source.Text, s.settings.MaxChunkBytes, s.settings.ChunkOverlapBytes)
	vectors, err := s.embed(ctx, chunks)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var created string
	_ = tx.QueryRowContext(ctx, `SELECT created_at FROM sources WHERE id=?`, source.ID).Scan(&created)
	if created == "" {
		created = now.Format(time.RFC3339Nano)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO sources(id,kind,path,title,collection,hash,media_type,text,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,path=excluded.path,title=excluded.title,
collection=excluded.collection,hash=excluded.hash,media_type=excluded.media_type,
text=excluded.text,updated_at=excluded.updated_at`,
		source.ID, source.Kind, source.Path, source.Title, source.Collection, source.Hash,
		source.MediaType, source.Text, created, now.Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM chunks_fts WHERE source_id=?`, source.ID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM chunks WHERE source_id=?`, source.ID); err != nil {
		return err
	}
	for i, chunk := range chunks {
		if _, err := tx.ExecContext(ctx, `INSERT INTO chunks(source_id,position,text) VALUES(?,?,?)`, source.ID, i, chunk); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO chunks_fts(source_id,position,text) VALUES(?,?,?)`, source.ID, i, chunk); err != nil {
			return err
		}
		if vectors != nil {
			raw, _ := json.Marshal(vectors[i])
			if _, err := tx.ExecContext(ctx, `INSERT INTO embeddings(source_id,position,vector) VALUES(?,?,?)`, source.ID, i, string(raw)); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	source.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	source.UpdatedAt = now
	return nil
}

func (s *Store) Search(ctx context.Context, req SearchRequest) ([]SearchResult, error) {
	if strings.TrimSpace(req.Query) == "" {
		return nil, errors.New("query is required")
	}
	collection, err := s.collection(req.Collection)
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit == 0 {
		limit = s.settings.MaxResults
	}
	if limit < 1 || limit > s.settings.MaxResults {
		return nil, errors.New("limit exceeds max_results")
	}
	candidateLimit := limit * 4
	rows, err := s.db.QueryContext(ctx, `
SELECT f.source_id,s.path,s.title,s.collection,CAST(f.position AS INTEGER),f.text,bm25(chunks_fts)
FROM chunks_fts f JOIN sources s ON s.id=f.source_id
WHERE chunks_fts MATCH ? AND s.collection=?
ORDER BY bm25(chunks_fts) LIMIT ?`, ftsQuery(req.Query), collection, candidateLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []SearchResult
	for rows.Next() {
		var item SearchResult
		var rank float64
		if err := rows.Scan(&item.SourceID, &item.Path, &item.Title, &item.Collection, &item.Position, &item.Text, &rank); err != nil {
			return nil, err
		}
		item.Score = -rank
		item.Citation = citation(item)
		results = append(results, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if s.embedder != nil && len(results) > 0 {
		queryVectors, err := s.embedder.embed(ctx, []string{req.Query})
		if err != nil {
			return nil, err
		}
		for i := range results {
			var raw string
			err := s.db.QueryRowContext(ctx, `SELECT vector FROM embeddings WHERE source_id=? AND position=?`,
				results[i].SourceID, results[i].Position).Scan(&raw)
			if err == nil {
				var vector []float64
				if json.Unmarshal([]byte(raw), &vector) == nil {
					results[i].Score = results[i].Score + cosine(queryVectors[0], vector)
				}
			}
		}
		slices.SortFunc(results, func(a, b SearchResult) int {
			if a.Score > b.Score {
				return -1
			}
			if a.Score < b.Score {
				return 1
			}
			return 0
		})
	}
	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func (s *Store) Get(ctx context.Context, id string) (Source, error) {
	query := `SELECT id,kind,path,title,collection,hash,media_type,text,created_at,updated_at FROM sources WHERE id=?`
	args := []any{id}
	if len(s.settings.Collections) > 0 {
		query += ` AND collection IN (` + placeholders(len(s.settings.Collections)) + `)`
		for _, c := range s.settings.Collections {
			args = append(args, c)
		}
	}
	var source Source
	var created, updated string
	err := s.db.QueryRowContext(ctx, query, args...).Scan(
		&source.ID, &source.Kind, &source.Path, &source.Title, &source.Collection,
		&source.Hash, &source.MediaType, &source.Text, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return Source{}, errors.New("knowledge source not found")
	}
	if err != nil {
		return Source{}, err
	}
	source.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	source.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	return source, nil
}

func (s *Store) Sources(ctx context.Context, req SourcesRequest) ([]Source, error) {
	collection, err := s.collection(req.Collection)
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("limit must be between 1 and 1000")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id,kind,path,title,collection,hash,media_type,created_at,updated_at
FROM sources WHERE collection=? ORDER BY updated_at DESC LIMIT ?`, collection, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Source
	for rows.Next() {
		var source Source
		var created, updated string
		if err := rows.Scan(&source.ID, &source.Kind, &source.Path, &source.Title, &source.Collection,
			&source.Hash, &source.MediaType, &created, &updated); err != nil {
			return nil, err
		}
		source.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		source.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
		result = append(result, source)
	}
	return result, rows.Err()
}

func (s *Store) Delete(ctx context.Context, id string) (map[string]any, error) {
	if !s.settings.AllowDelete {
		return nil, errors.New("knowledge deletion is disabled")
	}
	if len(s.settings.Collections) > 0 {
		var collection string
		err := s.db.QueryRowContext(ctx, `SELECT collection FROM sources WHERE id=?`, id).Scan(&collection)
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]any{"id": id, "deleted": false}, nil
		}
		if err != nil {
			return nil, err
		}
		if !slices.Contains(s.settings.Collections, collection) {
			return nil, fmt.Errorf("source %q belongs to collection %q which is not accessible", id, collection)
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM chunks_fts WHERE source_id=?`, id); err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM sources WHERE id=?`, id)
	if err != nil {
		return nil, err
	}
	count, _ := result.RowsAffected()
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "deleted": count > 0}, nil
}

func (s *Store) Reindex(ctx context.Context, collection string) (map[string]any, error) {
	collection, err := s.collection(collection)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM sources WHERE collection=? ORDER BY id`, collection)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		source, err := s.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if err := s.replace(ctx, source); err != nil {
			return nil, err
		}
	}
	return map[string]any{"collection": collection, "reindexed": len(ids)}, nil
}

func (s *Store) collection(value string) (string, error) {
	if value == "" {
		value = s.settings.Collections[0]
	}
	if !slices.Contains(s.settings.Collections, value) {
		return "", fmt.Errorf("collection %q is not allowed", value)
	}
	return value, nil
}

func (s *Store) embed(ctx context.Context, chunks []string) ([][]float64, error) {
	if s.embedder == nil {
		return nil, nil
	}
	return s.embedder.embed(ctx, chunks)
}

type embedder struct {
	baseURL   string
	model     string
	apiKeyEnv string
	client    *http.Client
}

func (e *embedder) embed(ctx context.Context, input []string) ([][]float64, error) {
	key := os.Getenv(e.apiKeyEnv)
	if key == "" {
		return nil, fmt.Errorf("embedding API key environment variable %s is empty", e.apiKeyEnv)
	}
	body, _ := json.Marshal(map[string]any{"model": e.model, "input": input})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+key)
	response, err := e.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("embedding provider returned %s", response.Status)
	}
	var decoded struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	result := make([][]float64, len(input))
	for _, item := range decoded.Data {
		if item.Index >= 0 && item.Index < len(result) {
			result[item.Index] = item.Embedding
		}
	}
	for i, vector := range result {
		if len(vector) == 0 {
			return nil, fmt.Errorf("embedding provider omitted input %d", i)
		}
	}
	return result, nil
}

func capability(name string, settings Settings) dispatcher.Capability {
	descriptions := map[string]string{
		Ingest: "Extract and index one allowed local document.", Put: "Create or replace an agent-authored local note.",
		Search: "Search indexed knowledge and return source citations.", Get: "Read one indexed source by id.",
		Sources: "List indexed sources in a collection.", Delete: "Delete one source and all derived chunks.",
		Reindex: "Rebuild all chunks and embeddings for a collection.",
	}
	schemas := map[string]string{
		Ingest:  `{"type":"object","properties":{"path":{"type":"string"},"collection":{"type":"string"}},"required":["path"],"additionalProperties":false}`,
		Put:     `{"type":"object","properties":{"id":{"type":"string"},"title":{"type":"string"},"text":{"type":"string"},"collection":{"type":"string"}},"required":["title","text"],"additionalProperties":false}`,
		Search:  `{"type":"object","properties":{"query":{"type":"string"},"collection":{"type":"string"},"limit":{"type":"integer","minimum":1}},"required":["query"],"additionalProperties":false}`,
		Get:     `{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}`,
		Sources: `{"type":"object","properties":{"collection":{"type":"string"},"limit":{"type":"integer","minimum":1}},"additionalProperties":false}`,
		Delete:  `{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}`,
		Reindex: `{"type":"object","properties":{"collection":{"type":"string"}},"additionalProperties":false}`,
	}
	return dispatcher.Capability{
		Name:        name,
		Description: descriptions[name] + " Collections: " + strings.Join(settings.Collections, ", "),
		InputSchema: json.RawMessage(schemas[name]),
	}
}

func isWrite(name string) bool {
	return name == Ingest || name == Put || name == Delete || name == Reindex
}

func canonicalRoots(values []string) ([]string, error) {
	var result []string
	for _, value := range values {
		abs, err := filepath.Abs(value)
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(abs)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("root %q is not a directory", value)
		}
		abs = filepath.Clean(abs)
		if !slices.Contains(result, abs) {
			result = append(result, abs)
		}
	}
	slices.Sort(result)
	return result, nil
}

func identifiers(values []string, label string) ([]string, error) {
	var result []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if !safeID(value) {
			return nil, fmt.Errorf("invalid %s %q", label, value)
		}
		if !slices.Contains(result, value) {
			result = append(result, value)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("at least one %s is required", label)
	}
	slices.Sort(result)
	return result, nil
}

func extensions(values []string) ([]string, error) {
	var result []string
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if !strings.HasPrefix(value, ".") {
			value = "." + value
		}
		if value == "." || strings.ContainsAny(value, `/\`) {
			return nil, fmt.Errorf("invalid extension %q", value)
		}
		if !slices.Contains(result, value) {
			result = append(result, value)
		}
	}
	slices.Sort(result)
	return result, nil
}

func safeID(value string) bool {
	if value == "" || len(value) > 200 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}

func validateBaseURL(value string) (string, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(value), "/"))
	if err != nil || parsed.Host == "" {
		return "", errors.New("invalid embedding_base_url")
	}
	if parsed.Scheme != "https" {
		host := parsed.Hostname()
		if parsed.Scheme != "http" || host != "localhost" && host != "127.0.0.1" && host != "::1" {
			return "", errors.New("embedding_base_url must use HTTPS or loopback HTTP")
		}
	}
	return parsed.String(), nil
}

func decodeStrict(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func chunkText(text string, size, overlap int) []string {
	if len(text) <= size {
		return []string{text}
	}
	var result []string
	for start := 0; start < len(text); {
		end := min(start+size, len(text))
		if end < len(text) {
			if newline := strings.LastIndex(text[start:end], "\n"); newline > size/2 {
				end = start + newline
			}
		}
		result = append(result, text[start:end])
		if end == len(text) {
			break
		}
		start = end - overlap
	}
	return result
}

func ftsQuery(query string) string {
	fields := strings.Fields(query)
	quoted := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.ReplaceAll(field, `"`, `""`)
		quoted = append(quoted, `"`+field+`"`)
	}
	return strings.Join(quoted, " AND ")
}

func citation(result SearchResult) string {
	if result.Path != "" {
		return fmt.Sprintf("%s#chunk-%d", result.Path, result.Position)
	}
	return fmt.Sprintf("%s#chunk-%d", result.SourceID, result.Position)
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func insideAny(path string, roots []string) bool {
	for _, root := range roots {
		rel, err := filepath.Rel(root, path)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

func cosine(a, b []float64) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, aa, bb float64
	for i := range a {
		dot += a[i] * b[i]
		aa += a[i] * a[i]
		bb += b[i] * b[i]
	}
	if aa == 0 || bb == 0 {
		return 0
	}
	return dot / (math.Sqrt(aa) * math.Sqrt(bb))
}
