# aurora-dispatchers-knowledge

Persistent local knowledge indexing and retrieval for Aurora.

Capabilities:

- `knowledge.ingest`
- `knowledge.put`
- `knowledge.search`
- `knowledge.get`
- `knowledge.sources`
- `knowledge.delete`
- `knowledge.reindex`

The dispatcher stores source documents, notes, chunks, and citations in SQLite
and uses FTS5 for lexical retrieval. Optional OpenAI-compatible embeddings add
vector reranking. API keys are read from a configured environment variable and
are never stored in manifests or SQLite.

```go
registry.New(knowledge.Registration{})
```

```json
{
  "name": "knowledge.ingest",
  "settings": {
    "database_path": "/home/user/.local/share/aurora/knowledge.db",
    "roots": ["/home/user/notes"],
    "collections": ["default", "work"],
    "allow_write": true
  }
}
```

To enable embeddings:

```json
{
  "embedding_base_url": "https://api.openai.com/v1",
  "embedding_model": "text-embedding-3-small",
  "embedding_api_key_env": "OPENAI_API_KEY"
}
```

Deletion and reindexing require approval by default. The repository depends on
`aurora-dispatchers-documents` for bounded local extraction.
