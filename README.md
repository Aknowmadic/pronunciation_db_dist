# Pronunciation DB — Distribution Package

English phonetic database: 220k words, 333k pronunciation variants, 35 Penn Treebank POS
tags, ARPAbet + IPA transcriptions, stress patterns, syllable counts, semantic relationships,
rhyme patterns, heteronym disambiguation, and G2P alignments.

## Quick Start

```bash
git clone https://github.com/YOUR_USERNAME/pronunciation_db_dist
cd pronunciation_db_dist
pip install pyarrow
python scripts/build.py        # downloads Parquet assets, rebuilds SQLite
python scripts/validate.py     # verifies the reconstruction
```

The reconstructed `pronunciation_db_reconstructed.db` is a fully-functional SQLite database
identical to the original.

---

## Database Statistics

| Table | Rows | Category |
|-------|------|----------|
| Words | 220,392 | large (Release asset) |
| Variants | 333,260 | large (Release asset) |
| SyllableInfo | 2,663,935 | large (Release asset) |
| WordPhonemes | 2,630,652 | large (Release asset) |
| SemanticRelationships | 711,222 | large (Release asset) |
| SynonymCache | 711,222 | large (Release asset) |
| G2PAlignments | 220,304 | large (Release asset) |
| Senses | 116,267 | large (Release asset) |
| Definitions | 115,840 | large (Release asset) |
| RhymePatterns | 57,459 | large (Release asset) |
| UsageExamples | 15,730 | large (Release asset) |
| PartOfSpeech | 43 | lookup (in-repo) |
| IpaAllowedChars | 611 | lookup (in-repo) |
| heteronym_groups | 116 | lookup (in-repo) |
| heteronym_pronunciations | 235 | lookup (in-repo) |
| Phonemes | 39 | lookup (in-repo) |
| … + 7 more lookups | — | lookup (in-repo) |

**POS system:** 35 Penn Treebank fine-grained tags (NN, NNP, VBD, VBG, JJ, RB, …)

---

## Repository Layout

```
pronunciation_db_dist/
├── README.md
├── requirements.txt
├── .gitignore
│
├── schema/
│   ├── schema.sql              ← Full SQLite DDL: tables, triggers, views, indexes
│   └── table_manifest.json     ← Row counts + SHA-256 checksums for all tables
│
├── data/
│   ├── lookups/                ← Small reference tables (<5k rows) — committed to git
│   │   ├── PartOfSpeech.parquet
│   │   ├── IpaAllowedChars.parquet
│   │   ├── heteronym_groups.parquet
│   │   ├── heteronym_pronunciations.parquet
│   │   ├── Phonemes.parquet
│   │   ├── Languages.parquet
│   │   └── … (14 more lookup tables)
│   │
│   ├── samples/                ← 1000-row samples of every table — committed to git
│   │   ├── Words_sample.parquet
│   │   ├── Variants_sample.parquet
│   │   └── … (one per table)
│   │
│   └── release/                ← NOT in git (.gitignore); large tables as GitHub Release assets
│       ├── Words.parquet           (2.2 MB compressed)
│       ├── Variants.parquet        (6.1 MB)
│       ├── SyllableInfo.parquet    (6.5 MB)
│       ├── WordPhonemes.parquet    (4.0 MB)
│       ├── SemanticRelationships.parquet (5.3 MB)
│       └── … (6 more)
│
└── scripts/
    ├── export.py               ← Rebuild Parquet files from a source SQLite DB
    ├── build.py                ← Download + reconstruct SQLite from scratch
    └── validate.py             ← Verify reconstruction correctness
```

---

## How It Works

### Distribution strategy

| Layer | What | Where |
|-------|------|-------|
| **Git repo** | Schema DDL, lookup tables, samples, scripts | GitHub (this repo) |
| **GitHub Release assets** | 11 large Parquet files (~37 MB total) | `Releases → v1.0.0` |

The raw SQLite database is **514 MB**.
The full Parquet distribution is **37 MB** — a **92% reduction** via ZSTD-9 columnar compression.

### Why Parquet?
- Column-oriented: only reads the columns you query
- ZSTD compression: best ratio for text-heavy linguistic data
- Type-safe: integer IDs, strings, floats all preserved exactly
- Universal: readable by Python, R, DuckDB, Spark, Julia, and every major analytics tool

### File format for each table

All Parquet files embed metadata:
```json
{
  "source_table": "Words",
  "source_db": "ultimate_2025_enhanced.db",
  "export_time": "1771488000"
}
```

Each file includes SHA-256 checksums in `schema/table_manifest.json` for tamper detection.

---

## Scripts

### `scripts/export.py` — Generate distribution from source DB

Run this if you have the original SQLite file and want to regenerate all Parquet files:

```bash
python scripts/export.py --db ultimate_2025_enhanced.db --out .
```

Options:
- `--db PATH` — source SQLite database
- `--out PATH` — output root (default: this repo's root)
- `--sample-size N` — rows to include in sample files (default: 1000)
- `--skip-large` — only export lookup tables (fast testing)

### `scripts/build.py` — Reconstruct SQLite

```bash
# Download from latest GitHub Release:
python scripts/build.py

# Pin a specific release tag:
python scripts/build.py --release v1.0.0

# Use local data/release/ directory (no network):
python scripts/build.py --local

# Custom output path:
python scripts/build.py --output my_pronunciation.db
```

The build process:
1. Applies `schema/schema.sql` (all tables, triggers, views, indexes)
2. Loads lookup Parquet from `data/lookups/`
3. Downloads large-table Parquet from GitHub Releases
4. Inserts all data with optimized WAL/cache SQLite pragmas
5. Validates FK integrity, row counts, and `PRAGMA integrity_check`

### `scripts/validate.py` — Verify reconstruction

```bash
python scripts/validate.py
python scripts/validate.py --db custom_output.db
```

Runs 4 validation suites:
1. Row count verification against manifest
2. Foreign key integrity check
3. 16 ground-truth spot-checks (known PTB tags, stress patterns, heteronym pairs)
4. View smoke-tests (Pronunciations, v_StressPatterns, v_RhymeFinder, etc.)

---

## Schema Highlights

### Penn Treebank POS (35 tags)

```sql
SELECT p.pos_abbreviation, COUNT(*) as words
FROM Words w
JOIN PartOfSpeech p ON w.part_of_speech = p.pos_id
WHERE p.is_active = 1
GROUP BY p.pos_abbreviation
ORDER BY words DESC;
-- NN: 67,249 | NNP: 42,351 | JJ: 29,292 | VB: 15,745 ...
```

### Heteronym stress-shift pairs

```sql
SELECT p.pos_abbreviation, v.arpabet, v.stress_pattern
FROM Variants v
JOIN Words w ON v.word_id = w.word_id
JOIN PartOfSpeech p ON w.part_of_speech = p.pos_id
WHERE LOWER(w.word) = 'desert';
-- NN: D EH1 Z ER0 T  stress=1-0  (DEsert — the noun)
-- VB: D IH0 Z ER1 T  stress=0-1  (deSERT — the verb)
```

### Rhyme lookup

```sql
SELECT * FROM v_RhymeFinder WHERE rhyme_key = 'AE1 T';
-- finds: cat, bat, rat, hat, mat, flat, ...
```

---

## Triggers & Referential Integrity

The database has 14 triggers preserving:
- IPA character validation on every `Variants` insert/update
- Bidirectional `SemanticRelationships` consistency
- `SynonymCache` / `AntonymCache` materialized views
- Timestamp auto-update on `Words`, `Variants`, `Definitions`

These triggers are stored in `schema/schema.sql` and are re-applied during reconstruction.

---

## Cite / Attribution

If you use this database in research, please cite it as:

```
Pronunciation DB v1.0.0 (2026). English phonetic database with PTB POS,
ARPAbet/IPA transcriptions, and heteronym disambiguation.
https://github.com/YOUR_USERNAME/pronunciation_db_dist
```

---

## License

See `LICENSE` file. Database content derived from public domain sources (CMU Pronouncing
Dictionary, Wiktionary, WordNet) with substantial original enrichment.
