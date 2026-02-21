# Pronunciation DB Analysis

## Overview

This repository contains the distribution package for a large English phonetic database. It is designed to be reconstructed into a SQLite database from a set of Parquet files.

## Architecture

The project uses a hybrid distribution model:
-   **Schema & Scripts**: Stored in the Git repository.
-   **Lookup Tables**: Small reference tables (<5k rows) stored as Parquet files in `data/lookups/`.
-   **Large Tables**: Large data tables (>5k rows) are stored as GitHub Release assets (not in the Git history) to keep the repository size manageable.

## Key Components

### Scripts
-   `scripts/build.py`: Reconstructs the SQLite database from the Parquet files. It downloads the large tables from GitHub Releases and combines them with the local lookup tables.
-   `scripts/validate.py`: Verifies the integrity of the reconstructed database using row counts, foreign key checks, and spot checks.
-   `scripts/export.py`: Exports an existing SQLite database into the Parquet distribution format, generating the schema and manifest.

### Schema
The database schema (`schema/schema.sql`) is comprehensive and includes:
-   **Core Data**: `Words`, `Variants` (pronunciations), `Definitions`, `Senses`.
-   **Phonetics**: `Phonemes`, `SyllableInfo`, `WordPhonemes`, `G2PAlignments`.
-   **Semantics**: `SemanticRelationships` (synonyms, antonyms, etc.) with bidirectional consistency triggers.
-   **Metadata**: `Dialects`, `Registers`, `Regions`, `PartOfSpeech`.
-   **Integrity**: Extensive use of Foreign Keys and Triggers to ensure data quality (e.g., validating IPA characters, maintaining symmetric relationships).

### Data Statistics
Based on `schema/table_manifest.json`, the database contains:
-   ~220k Words
-   ~333k Pronunciation Variants
-   ~2.6M Syllable Info records
-   ~2.6M Word Phoneme records
-   ~711k Semantic Relationships

## Usage

To use this database, you would typically:
1.  Clone the repository.
2.  Install dependencies (`pyarrow` or `pandas`).
3.  Run `python scripts/build.py` to download the data and build the `pronunciation_db_reconstructed.db` SQLite file.
4.  Use `python scripts/validate.py` to ensure the build is correct.

## Observations

-   The repository currently contains the `lookups` and `samples` directories, but not the `release` directory (which is expected as it's downloaded).
-   The schema is well-designed with strong data integrity constraints.
-   The use of Parquet allows for efficient storage and distribution of the large dataset.
