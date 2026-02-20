"""
build.py — Reconstruct SQLite from Parquet distribution
=========================================================
Downloads large-table Parquet files from a GitHub Release (or a local
data/release/ directory), combines them with the in-git lookup Parquet files,
applies the schema DDL, and reconstructs a fully-functional SQLite database.

Usage:
    # Download from latest GitHub Release:
    python scripts/build.py

    # Pin a specific release tag:
    python scripts/build.py --release v1.0.0

    # Use a local data/release/ directory (no download):
    python scripts/build.py --local

    # Choose output path:
    python scripts/build.py --output my_pronunciation.db
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Optional dependency: pyarrow / pandas
# ---------------------------------------------------------------------------
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

if not HAS_PYARROW and not HAS_PANDAS:
    sys.exit("ERROR: pip install pyarrow  (or pandas as fallback)")

# ---------------------------------------------------------------------------
# Configuration — update GITHUB_REPO before publishing
# ---------------------------------------------------------------------------
GITHUB_REPO  = "Aknowmadic/pronunciation_db_dist"
RELEASE_TAG  = "latest"                                 # overridden by --release

OUT_DEFAULT  = Path("pronunciation_db_reconstructed.db")
ROOT_DEFAULT = Path(__file__).resolve().parent.parent   # pronunciation_db_dist/

# SQLite write pragmas for fast bulk loading
FAST_PRAGMAS = [
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA cache_size = -131072",   # 128 MB page cache
    "PRAGMA temp_store = MEMORY",
    "PRAGMA mmap_size = 536870912",  # 512 MB mmap
    "PRAGMA foreign_keys = OFF",     # disabled during load; re-enabled + checked at end
]

RESTORE_PRAGMAS = [
    "PRAGMA foreign_keys = ON",
    "PRAGMA integrity_check",
]

CHUNK_SIZE = 10_000  # rows per INSERT batch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download_file(url: str, dest: Path, expected_sha256: str | None = None) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  Downloading {url}")

    req = urllib.request.Request(url, headers={"User-Agent": "pronunciation-db-builder/1.0"})
    try:
        with urllib.request.urlopen(req) as resp, open(dest, "wb") as fh:
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            while chunk := resp.read(1024 * 1024):
                fh.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"\r    {pct:.0f}%  ({human(downloaded)} / {human(total)})  ", end="", flush=True)
        print()
    except urllib.error.HTTPError as e:
        sys.exit(f"\nERROR downloading {url}: {e}")

    if expected_sha256:
        actual = sha256_file(dest)
        if actual != expected_sha256:
            dest.unlink(missing_ok=True)
            sys.exit(f"ERROR: checksum mismatch for {dest.name}\n  expected {expected_sha256}\n  got      {actual}")
        print(f"    SHA-256 verified ✓")


def github_release_asset_url(repo: str, tag: str, filename: str) -> str:
    if tag == "latest":
        return f"https://github.com/{repo}/releases/latest/download/{filename}"
    return f"https://github.com/{repo}/releases/download/{tag}/{filename}"


def read_parquet(path: Path) -> list[tuple]:
    """Read a Parquet file, returning (column_names, rows_as_tuples)."""
    if HAS_PYARROW:
        tbl = pq.read_table(str(path))
        cols = tbl.schema.names
        # Convert to list of tuples for SQLite executemany
        rows = list(zip(*[tbl.column(c).to_pylist() for c in cols]))
        return cols, rows
    else:
        df = pd.read_parquet(str(path))
        return list(df.columns), [tuple(r) for r in df.itertuples(index=False)]


def insert_table(conn: sqlite3.Connection, table: str, parquet_path: Path) -> int:
    """Load a Parquet file into the named SQLite table. Returns row count."""
    cols, rows = read_parquet(parquet_path)
    if not rows:
        return 0

    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(f'"{c}"' for c in cols)
    sql = f'INSERT OR IGNORE INTO "{table}" ({col_list}) VALUES ({placeholders})'

    cur = conn.cursor()
    inserted = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        batch = rows[i : i + CHUNK_SIZE]
        cur.executemany(sql, batch)
        inserted += cur.rowcount if cur.rowcount >= 0 else len(batch)

    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Reconstruct SQLite from Parquet distribution")
    parser.add_argument("--release", default=RELEASE_TAG, help="GitHub release tag (default: latest)")
    parser.add_argument("--output",  default=str(OUT_DEFAULT), help="Output SQLite path")
    parser.add_argument("--root",    default=str(ROOT_DEFAULT), help="pronunciation_db_dist root dir")
    parser.add_argument("--local",   action="store_true", help="Use local data/release/ instead of downloading")
    parser.add_argument("--repo",    default=GITHUB_REPO, help="GitHub repo (owner/name)")
    args = parser.parse_args()

    root       = Path(args.root)
    out_path   = Path(args.output)
    schema_sql = root / "schema" / "schema.sql"
    manifest_p = root / "schema" / "table_manifest.json"

    if not schema_sql.exists():
        sys.exit(f"ERROR: schema not found at {schema_sql}")
    if not manifest_p.exists():
        sys.exit(f"ERROR: manifest not found at {manifest_p}")

    manifest: dict[str, Any] = json.loads(manifest_p.read_text(encoding="utf-8"))

    # --- Create / clear output DB ---
    if out_path.exists():
        out_path.unlink()
    conn = sqlite3.connect(str(out_path))

    print("=== Pronunciation DB Reconstruction ===")
    print(f"  Output: {out_path}")

    # Apply fast write pragmas
    for pragma in FAST_PRAGMAS:
        conn.execute(pragma)

    # --- Apply schema DDL ---
    print("\n[1/3] Applying schema DDL...")
    ddl = schema_sql.read_text(encoding="utf-8")
    # Strip PRAGMA lines (already applied above)
    ddl_clean = "\n".join(
        line for line in ddl.splitlines()
        if not line.strip().upper().startswith("PRAGMA")
    )
    conn.executescript(ddl_clean)
    print(f"  Schema applied from {schema_sql.name}")

    # --- Resolve Parquet files ---
    print("\n[2/3] Loading data...")
    tables_in_manifest = manifest["tables"]
    tmp_dir = root / "data" / "_tmp_downloaded"

    for table, info in tables_in_manifest.items():
        category    = info["category"]
        expected_sha = info.get("sha256")
        parquet_rel = info["parquet_path"]

        if category == "lookup":
            parquet_path = root / parquet_rel
            if not parquet_path.exists():
                print(f"  WARN: lookup file missing: {parquet_path} — skipping {table}")
                continue
        else:  # large table
            if args.local:
                parquet_path = root / "data" / "release" / f"{table}.parquet"
                if not parquet_path.exists():
                    print(f"  WARN: local file missing: {parquet_path} — skipping {table}")
                    continue
            else:
                parquet_path = tmp_dir / f"{table}.parquet"
                if not parquet_path.exists() or (expected_sha and sha256_file(parquet_path) != expected_sha):
                    url = github_release_asset_url(args.repo, args.release, f"{table}.parquet")
                    download_file(url, parquet_path, expected_sha)

        t0 = time.time()
        n = insert_table(conn, table, parquet_path)
        elapsed = time.time() - t0
        size = parquet_path.stat().st_size
        print(f"  [{category.upper()[:6]:6}] {table:40} {n:>10,} rows  ({human(size):>8}, {elapsed:.1f}s)")

    # --- Post-load: restore integrity pragmas ---
    print("\n[3/3] Validating integrity...")
    conn.execute("PRAGMA foreign_keys = ON")

    fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_errors:
        print(f"  WARN: {len(fk_errors)} foreign key violation(s):")
        for e in fk_errors[:10]:
            print(f"    {e}")
    else:
        print("  Foreign key check: PASS")

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"  Integrity check:   {integrity.upper()}")

    # Row count spot-check
    cur = conn.cursor()
    mismatches = 0
    for table, info in tables_in_manifest.items():
        expected = info["rows"]
        if expected == 0:
            continue
        actual = cur.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if actual != expected:
            print(f"  MISMATCH {table}: expected {expected:,}, got {actual:,}")
            mismatches += 1
    if mismatches == 0:
        print(f"  Row counts:        PASS ({len(tables_in_manifest)} tables verified)")

    conn.execute("PRAGMA wal_checkpoint(FULL)")
    conn.close()

    size_mb = out_path.stat().st_size / 1e6
    print(f"\nDone. Reconstructed DB: {out_path} ({size_mb:.0f} MB)")

    if fk_errors or integrity != "ok" or mismatches:
        print("WARNING: Validation found issues — see above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
