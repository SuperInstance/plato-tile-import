# Architecture: plato-tile-import

## Language Choice: Rust

### Why Rust

Import is throughput-bound. The bottleneck is JSON parsing + validation + dedup.
Rust's serde is the fastest JSON parser in the ecosystem.

| Metric | Python (json + dict) | Rust (serde) |
|--------|---------------------|--------------|
| Parse 10K JSON tiles | ~200ms | ~25ms |
| Validate 10K tiles | ~50ms | ~8ms |
| Dedup 10K tiles | ~80ms | ~5ms (HashSet) |

**8x overall speedup** on the import pipeline.

### Why not Pydantic

Pydantic is great for Python validation. But every model instance is a Python
object (~500 bytes overhead). For 100K tiles: ~50MB Python vs ~5MB Rust.

### Why not Apache Arrow

Arrow is amazing for columnar data. But tile imports are row-oriented
(one tile at a time). Arrow's advantage is batch column operations.
We'd add Arrow for bulk analytics exports.

### Architecture

```
JSON/Vec → validate() → dedup check → transform chain → ImportTile
                ↓                    ↓
           ValidationResult    seen_ids / seen_content (HashSet)
```

### Dedup Strategy

Two-level dedup:
1. **ID dedup**: exact ID match (fast, HashSet lookup)
2. **Content dedup**: FNV-1a hash of content (catches re-imports with new IDs)

This catches both exact re-imports and content-level duplicates without
expensive full-text comparison.
