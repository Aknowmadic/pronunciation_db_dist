"""
export.py — SQLite → Parquet distribution exporter
====================================================
Splits ultimate_2025_enhanced.db into per-table Parquet files with ZSTD
compression, extracts the full DDL schema, generates a manifest with
SHA-256 checksums, and writes 1 000-row samples for quick inspection.

Usage:
    python scripts/export.py --db <path_to_db> [--out <output_root>] [--sample-size 1000]

Output layout:
    data/lookups/<Table>.parquet   — tables with <IN_GIT_THRESHOLD rows (committed to git)
    data/samples/<Table>_sample.parquet — first N rows of every table (committed to git)
    data/release/<Table>.parquet   — large tables (upload to GitHub Release)
    schema/schema.sql              — full SQLite DDL (tables + triggers + views + indexes)
    schema/table_manifest.json     — row counts, file sizes, SHA-256 checksums
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Optional dependency: pyarrow.  Fall back to pandas-only if absent.
# ---------------------------------------------------------------------------
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

if not HAS_PYARROW and not HAS_PANDAS:
    sys.exit("ERROR: install pyarrow (recommended) or pandas to run this script.\n  pip install pyarrow pandas")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_DEFAULT = r"C:\Users\Ahmed\OneDrive\Desktop\aiprojects\NLP\data\Computational Linguistics\ultimate_2025_enhanced.db"
OUT_DEFAULT = Path(__file__).resolve().parent.parent  # pronunciation_db_dist/

# Tables with <= this many rows are "lookup" tables — stored in git.
IN_GIT_ROW_THRESHOLD = 5_000

# Per-table full SELECT overrides to exclude orphaned rows caused by FK enforcement
# being OFF at runtime during the PTB retag migration.  Uses JOINs (not IN subqueries)
# so that SQLite uses the existing word_id indexes and runs in O(N log N) not O(N^2).
# Set to None to use plain SELECT *.
INTEGRITY_SELECT: dict[str, str] = {
    "Variants": (
        'SELECT v.* FROM "Variants" v '
        'INNER JOIN "Words" w ON v.word_id = w.word_id'
    ),
    "SemanticRelationships": (
        'SELECT sr.* FROM "SemanticRelationships" sr '
        'INNER JOIN "Words" w1 ON sr.source_word_id = w1.word_id '
        'INNER JOIN "Words" w2 ON sr.target_word_id = w2.word_id'
    ),
    "SynonymCache": (
        'SELECT sc.* FROM "SynonymCache" sc '
        'INNER JOIN "Words" w1 ON sc.word_id = w1.word_id '
        'INNER JOIN "Words" w2 ON sc.synonym_word_id = w2.word_id'
    ),
}

# Tables that are always excluded from export (SQLite internals or empty cache tables).
SKIP_TABLES = {
    "sqlite_sequence",          # SQLite internal — auto-rebuilt
    "AntonymCache",             # Empty; derived from SemanticRelationships via trigger
    "CompoundWordParts",        # Empty
    "EmbeddingModels",          # Empty
    "MaintenanceLogs",          # Empty
    "MorphologicalForms",       # Empty
    "SenseEmbeddings",          # Empty
    "TestCaseResults",          # Empty test infrastructure
    "TestRuns",                 # Empty test infrastructure
    "WordEmbeddings",           # Empty
}

# ZSTD compression level (1–22).  Level 9 gives excellent ratio with fast decompression.
ZSTD_LEVEL = 9

# Row-group size for Parquet writer (controls read granularity & memory during load).
ROW_GROUP_SIZE = 50_000

SAMPLE_SIZE_DEFAULT = 1_000

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def sqlite_type_to_arrow(declared_type: str) -> pa.DataType:
    """Map SQLite declared column type to a safe Arrow type."""
    dt = (declared_type or "").upper().split("(")[0].strip()
    if dt in ("INTEGER", "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
               "UNSIGNED BIG INT", "INT2", "INT8"):
        return pa.int64()
    if dt in ("REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT"):
        return pa.float64()
    if dt == "BLOB":
        return pa.binary()
    # TEXT, VARCHAR, CHAR, CLOB, NCHAR, NVARCHAR, or anything else → string
    return pa.string()


def build_arrow_schema(conn: sqlite3.Connection, table: str) -> pa.Schema:
    """Build an explicit Arrow schema from PRAGMA table_info, avoiding null-type inference."""
    cur = conn.cursor()
    cols = cur.execute(f'PRAGMA table_info("{table}")').fetchall()
    # cols: (cid, name, type, notnull, dflt_value, pk)
    fields = [pa.field(col[1], sqlite_type_to_arrow(col[2])) for col in cols]
    return pa.schema(fields).with_metadata({
        b"source_table": table.encode(),
        b"source_db": b"ultimate_2025_enhanced.db",
        b"export_time": str(int(time.time())).encode(),
    })


def build_select_sql(table: str, limit: int | None = None) -> str:
    """Build SELECT statement using an indexed JOIN override if available, otherwise SELECT *."""
    base = INTEGRITY_SELECT.get(table) or f'SELECT * FROM "{table}"'
    if limit:
        base += f" LIMIT {limit}"
    return base


def export_table_pyarrow(
    conn: sqlite3.Connection,
    table: str,
    out_path: Path,
    limit: int | None = None,
) -> int:
    """Export *table* to a Parquet file using pyarrow.  Returns row count."""
    sql = build_select_sql(table, limit)

    # Derive schema from PRAGMA (not from data) to avoid null-type mismatches
    # when early chunks have all-NULL values in a column.
    arrow_schema = build_arrow_schema(conn, table)
    col_names = [f.name for f in arrow_schema]

    chunk_size = 100_000
    total = 0

    writer = pq.ParquetWriter(
        str(out_path),
        arrow_schema,
        compression="zstd",
        compression_level=ZSTD_LEVEL,
    )

    cursor = conn.cursor()
    cursor.execute(sql)

    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        # Build arrays, casting to the declared Arrow type to handle mixed-type SQLite data.
        arrays = []
        for i, field in enumerate(arrow_schema):
            raw = [r[i] for r in rows]
            try:
                arr = pa.array(raw, type=field.type, safe=False)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                # Fallback: cast via string for problematic columns
                arr = pa.array([str(v) if v is not None else None for v in raw],
                               type=pa.string()).cast(field.type, safe=False)
            arrays.append(arr)
        batch = pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)
        writer.write_batch(batch)
        total += len(rows)

    writer.close()
    return total


def export_table_pandas(
    conn: sqlite3.Connection,
    table: str,
    out_path: Path,
    limit: int | None = None,
) -> int:
    """Fallback: export via pandas if pyarrow is not available."""
    import pandas as pd
    sql = build_select_sql(table, limit)
    df = pd.read_sql_query(sql, conn)
    df.to_parquet(str(out_path), index=False, compression="zstd")
    return len(df)


def export_table(conn: sqlite3.Connection, table: str, out_path: Path, limit: int | None = None) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if HAS_PYARROW:
        return export_table_pyarrow(conn, table, out_path, limit)
    return export_table_pandas(conn, table, out_path, limit)


def extract_schema(conn: sqlite3.Connection, out_path: Path) -> None:
    """
    Dump DDL for all user-created objects (tables, triggers, views, indexes)
    in dependency order.  Excludes sqlite_* internal tables and auto-index objects.
    """
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT type, name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND name NOT LIKE 'sqlite_%'
        ORDER BY
          CASE type
            WHEN 'table'   THEN 1
            WHEN 'index'   THEN 2
            WHEN 'trigger' THEN 3
            WHEN 'view'    THEN 4
            ELSE 5
          END,
          name
    """).fetchall()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("-- ============================================================\n")
        f.write("-- ultimate_2025_enhanced.db — full DDL\n")
        f.write("-- Generated by scripts/export.py\n")
        f.write("-- ============================================================\n\n")
        f.write("PRAGMA journal_mode = WAL;\n")
        f.write("PRAGMA foreign_keys = OFF;  -- re-enabled by build.py after bulk load\n\n")

        current_type = None
        for obj_type, name, sql in rows:
            if obj_type != current_type:
                current_type = obj_type
                f.write(f"\n-- ---- {obj_type.upper()}S ----\n\n")
            f.write(f"{sql.strip()};\n\n")

    print(f"  Schema -> {out_path} ({out_path.stat().st_size:,} bytes)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Export SQLite DB to Parquet distribution")
    parser.add_argument("--db", default=DB_DEFAULT, help="Path to source SQLite database")
    parser.add_argument("--out", default=str(OUT_DEFAULT), help="Output root directory")
    parser.add_argument("--sample-size", type=int, default=SAMPLE_SIZE_DEFAULT)
    parser.add_argument("--skip-large", action="store_true", help="Only export lookup tables (faster testing)")
    args = parser.parse_args()

    db_path = Path(args.db)
    out_root = Path(args.out)

    if not db_path.exists():
        sys.exit(f"ERROR: database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # --- Enumerate tables with row counts ---
    all_tables = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    table_info: list[dict[str, Any]] = []
    for (name,) in all_tables:
        if name in SKIP_TABLES:
            continue
        count = cur.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        category = "lookup" if count <= IN_GIT_ROW_THRESHOLD else "large"
        table_info.append({"table": name, "rows": count, "category": category})

    print(f"Found {len(table_info)} tables to export ({sum(1 for t in table_info if t['category']=='lookup')} lookup, "
          f"{sum(1 for t in table_info if t['category']=='large')} large)")

    # --- Extract schema ---
    print("\n[1/3] Extracting schema...")
    extract_schema(conn, out_root / "schema" / "schema.sql")

    # --- Export tables ---
    print(f"\n[2/3] Exporting tables (pyarrow={HAS_PYARROW})...")
    manifest: dict[str, Any] = {"tables": {}, "export_meta": {
        "source_db": db_path.name,
        "zstd_level": ZSTD_LEVEL,
        "in_git_threshold": IN_GIT_ROW_THRESHOLD,
        "pyarrow_used": HAS_PYARROW,
    }}

    for info in table_info:
        table = info["table"]
        rows = info["rows"]
        category = info["category"]

        if args.skip_large and category == "large":
            print(f"  SKIP  {table:40} ({rows:>10,} rows) — --skip-large")
            continue

        # Main export
        dest_dir = out_root / "data" / ("lookups" if category == "lookup" else "release")
        dest = dest_dir / f"{table}.parquet"

        t0 = time.time()
        exported = export_table(conn, table, dest)
        elapsed = time.time() - t0

        size = dest.stat().st_size if dest.exists() else 0
        checksum = sha256_file(dest) if dest.exists() else ""

        # Sample export (always written to data/samples/, regardless of category)
        sample_dest = out_root / "data" / "samples" / f"{table}_sample.parquet"
        if rows > 0 and args.sample_size > 0:
            sample_rows = min(args.sample_size, rows)
            export_table(conn, table, sample_dest, limit=sample_rows)
        elif rows == 0:
            # Write empty sample
            export_table(conn, table, sample_dest, limit=0)

        manifest["tables"][table] = {
            "rows": exported,
            "category": category,
            "parquet_path": str(dest.relative_to(out_root)).replace("\\", "/"),
            "sample_path": str(sample_dest.relative_to(out_root)).replace("\\", "/"),
            "size_bytes": size,
            "size_human": human(size),
            "sha256": checksum,
        }

        tag = "LOOKUP" if category == "lookup" else "LARGE "
        print(f"  [{tag}] {table:40} {exported:>10,} rows -> {human(size):>8}  ({elapsed:.1f}s)")

    # --- Write manifest ---
    print("\n[3/3] Writing manifest...")
    manifest_path = out_root / "schema" / "table_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"  Manifest -> {manifest_path}")

    # --- Summary ---
    total_size = sum(
        info.get("size_bytes", 0) for info in manifest["tables"].values()
    )
    large_size = sum(
        info["size_bytes"] for info in manifest["tables"].values()
        if info["category"] == "large"
    )
    lookup_size = total_size - large_size

    print("\n=== Export Summary ===")
    print(f"  Total Parquet size : {human(total_size)}")
    print(f"  In-git (lookups)   : {human(lookup_size)}")
    print(f"  GitHub Release     : {human(large_size)}")
    print(f"  Original DB        : {human(db_path.stat().st_size)}")
    ratio = (1 - total_size / db_path.stat().st_size) * 100
    print(f"  Space saving       : {ratio:.0f}%")
    print()
    print("Next steps:")
    print("  1. Commit data/lookups/, data/samples/, schema/ to git")
    print("  2. Run:  gh release create v1.0.0 data/release/*.parquet --title 'DB v1.0.0'")
    print("  3. Update GITHUB_REPO in scripts/build.py")

    conn.close()


if __name__ == "__main__":
    main()
