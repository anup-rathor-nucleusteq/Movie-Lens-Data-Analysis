# Silver Layer — Process & Dataflow Documentation

**Project:** MovieLens 32M Data Engineering Capstone
**Layer:** Silver (cleaned, validated, conformed data)
**Source:** `bronze.*` tables (Postgres)
**Orchestration:** Airflow DAG `silver_transform`
**Trigger:** Automatic, via Airflow Dataset `BRONZE_READY` (fires when `bronze_ingestion` DAG completes successfully)

---

## 1. Purpose

Bronze holds raw data exactly as it arrived from the source CSVs — no
validation, no type correction, no deduplication. Silver is where that
raw data becomes trustworthy: nulls are checked, invalid values are
separated out, timestamps become real dates, column names follow the
project's PascalCase standard, and two audit columns (`CreateDtTm`,
`UpdateDtTm`) are added to every table.

Nothing is ever silently dropped. Rows that fail validation are
written to a parallel `quarantine.*` table with a reason code, so the
count of `silver + quarantine` always equals `bronze` for every table
built directly from a bronze source.

---

## 2. Codebase Layout

| File | Role |
|---|---|
| `src/python/config.py` | Environment-driven settings (DB connection, paths) — shared with bronze |
| `src/python/db.py` | Small Postgres operations: connect, execute, count rows |
| `src/python/spark_utils.py` | Spark session factory, CSV/JDBC read helpers, `write_to_postgres` |
| `src/python/transforms.py` | Reusable column-level transformations |
| `src/python/quality.py` | Validation rules and quarantine logic |
| `src/pyspark/build_silver.py` | The build job — one function per table |
| `airflow_dags/dag_silver_transform.py` | Airflow DAG wiring the build functions into tasks |
| `airflow_dags/datasets.py` | Shared `BRONZE_READY` / `SILVER_READY` dataset labels |
| `sql/gold_queries.sql` | (referenced by gold layer, not silver) |

---

## 3. Transformations Applied (`transforms.py`)

| Function | What it does | Used on |
|---|---|---|
| `extract_release_year` | Pulls the 4-digit year out of a title string using the regex `\((\d{4})\)\s*$`, anchored to the end of the string so it never matches a year embedded elsewhere in the title (e.g. `1984 (1956)` correctly returns 1956, not 1984) | `movies` |
| `clean_title` | Strips the `(YYYY)` suffix, producing a year-free title | `movies` |
| `epoch_to_timestamp` | Converts epoch-seconds integers to a real UTC timestamp using `F.timestamp_seconds()` | `ratings`, `tags` |
| `add_audit_columns` | Adds `CreateDtTm` and `UpdateDtTm`, both set to `current_timestamp()` at write time | every silver table |
| `rename_columns` | Renames bronze's camelCase columns (`movieId`) to silver's PascalCase (`MovieId`) | every silver table |
| `select_columns` | Enforces final column order for each table | every silver table |
| `dedupe_keep_latest` | Removes duplicate rows on a key using `row_number()` over a window ordered by a timestamp, keeping only the most recent | `ratings` only |
| `aggregate_to_string` | Groups rows and joins a text column into one pipe-delimited string per group, using `collect_list` + `concat_ws` | `tags`, before joining into `user_ratings_master` |

---

## 4. Validation Rules Applied (`quality.py`)

| Rule | Condition flagged as bad | Applied to |
|---|---|---|
| Null key check | Any of the specified key columns is null | all tables |
| Rating range | `Rating < 0.5` or `Rating > 5.0` | `ratings` |
| Timestamp range | Epoch value outside the dataset's documented window (1995-01-09 to 2023-10-13, with a small buffer on each end to include the full first and last day) | `ratings`, `tags` |

Each rule returns a boolean Spark condition. `split_good_and_bad()`
filters the DataFrame into two: rows that pass go to silver, rows that
fail are tagged with a `RejectReason` string and written to
`quarantine.<table_name>`.

**Note on a boundary bug found and fixed during development:** the
initial epoch range constants were set to the very start of the
dataset's final day rather than its end, which caused 109 valid
late-day ratings to be incorrectly quarantined. The constants were
corrected and a full re-run confirmed 0 rows quarantined.

---

## 5. Table-by-Table Dataflow

### 5.1 `silver.links`

```
bronze.links
  → rename (movieId→MovieId, imdbId→ImdbId, tmdbId→TmdbId)
  → validate: MovieId not null
  → split good/bad
  → add CreateDtTm, UpdateDtTm
  → write silver.links (overwrite)
  → write quarantine.links (overwrite)
```

Small table (87,585 rows), read and written in a single pass — no
batching required.

`ImdbId` is kept as a string throughout the pipeline (bronze → silver)
because the values are zero-padded (`0114709`); casting to an integer
would silently destroy the leading zeros.

### 5.2 `silver.movies`

```
bronze.movies
  → extract_release_year(title)      -- adds ReleaseYear
  → clean_title(title)               -- adds CleanTitle
  → rename (movieId→MovieId, title→Title, genres→Genres)
  → validate: MovieId, Title not null
  → split good/bad
  → add CreateDtTm, UpdateDtTm
  → write silver.movies (overwrite)
  → write quarantine.movies (overwrite)
```

Both `Title` (original, with year) and `CleanTitle` (year-free) are
retained in this table. `Title` here is the authoritative original
string; downstream tables (`movie_metadata`, `user_ratings_master`)
alias `CleanTitle` as `Title` so dashboards never show a duplicated
year in the display name.

### 5.3 `silver.ratings`

```
bronze.ratings (read in parallel chunks by UserId, batched)
  → rename (userId→UserId, movieId→MovieId, rating→Rating)
  → validate: null keys, rating range, timestamp range
  → split good/bad
  → epoch_to_timestamp(timestamp → RatingTstmp)
  → dedupe_keep_latest on (UserId, MovieId), ordered by timestamp desc
  → add CreateDtTm, UpdateDtTm
  → write silver.ratings (batch 1 = overwrite, batches 2+ = append)
  → write quarantine.ratings (same batch mode)
```

32,000,204 rows. Read and written in batches of 20,000 users at a
time (≈3M rows/batch) to keep memory usage flat regardless of total
table size. Deduplication happens *after* the good/bad split, so a
bad row is never allowed to "win" a dedupe tie against a valid one.

**Deduplication is applied here** because a user rating the same
movie twice is a genuine duplicate — `(UserId, MovieId)` is expected
to be unique.

### 5.4 `silver.tags`

```
bronze.tags (read in parallel chunks by UserId, batched)
  → rename (userId→UserId, movieId→MovieId, tag→TagText)
  → validate: null keys, timestamp range
  → split good/bad
  → epoch_to_timestamp(timestamp → TagTstmp)
  → add CreateDtTm, UpdateDtTm
  → write silver.tags (batch 1 = overwrite, batches 2+ = append)
  → write quarantine.tags (same batch mode)
```

2,000,072 rows. **No deduplication is applied** — a user can
legitimately apply multiple distinct tags to the same movie (e.g.
`action`, `assassin`, `James Bond` all on one film). Deduplicating on
`(UserId, MovieId)` here would destroy the majority of real tag data.
This is a deliberate divergence from the ratings table's handling and
is called out explicitly so it is never "corrected" by mistake later.

### 5.5 `silver.movie_metadata`

**Not built directly from bronze** — this is a derived table, joined
from two already-cleaned silver tables.

```
silver.movies (select MovieId, Title, CleanTitle, ReleaseYear, Genres)
  LEFT JOIN silver.links (select MovieId, ImdbId, TmdbId)
    ON MovieId
  → drop original Title, rename CleanTitle → Title
  → add CreateDtTm, UpdateDtTm
  → write silver.movie_metadata (overwrite)
```

87,585 rows — one row per movie. A **LEFT join** is used deliberately:
if a movie has no corresponding link row, the movie is still kept with
null `ImdbId`/`TmdbId` rather than being silently dropped, which an
inner join would do.

The audit columns from `links` are dropped before the join
(`links.select("MovieId","ImdbId","TmdbId")`) — otherwise both sides
of the join carry `CreateDtTm`/`UpdateDtTm` and Spark cannot
disambiguate which one a later reference to that column name means.

The `Title` column in this table is the **year-free** version
(`CleanTitle` aliased as `Title`), so it flows into
`user_ratings_master` and all gold tables without any year duplicated
inside the display string. `silver.movies.Title` remains the original
source-of-truth value with the year included.

### 5.6 `silver.user_ratings_master`

**The largest derived table.** Built by joining ratings to metadata
and to an aggregated view of tags.

```
For each batch of 20,000 users:
  silver.ratings   (batch, select UserId, MovieId, Rating, RatingTstmp)
  silver.tags      (same batch of users)
      → aggregate_to_string, grouped by (UserId, MovieId),
        joining TagText values with "|" into one Tags column
        -- this step happens BEFORE the join, specifically to prevent
        -- one rating row exploding into N rows (one per tag) if a
        -- user tagged the same movie multiple times
  silver.movie_metadata (cached once, reused across all batches)
      → select MovieId, Title, ReleaseYear, Genres, ImdbId, TmdbId

  ratings LEFT JOIN metadata ON MovieId
          LEFT JOIN aggregated_tags ON (UserId, MovieId)
  → add CreateDtTm, UpdateDtTm
  → write silver.user_ratings_master (batch 1 = overwrite, batches 2+ = append)
```

32,000,204 rows — exactly matching `silver.ratings`, confirming the
tag aggregation step successfully prevented row multiplication.

`movie_metadata` is read once and `.cache()`d before the batch loop
begins, rather than re-read from Postgres on every one of the ~11
batches — it is a small (87,585-row) table reused unchanged across the
whole build.

Both joins are LEFT joins: a rating must survive in this table even if
the movie has no metadata row or the user left no tags on that movie
(the common case — most ratings have no associated tag at all, so
`Tags` is `NULL` for the majority of rows).

---

## 6. Idempotency & Safe Re-runs

Every table in this layer uses `mode="overwrite"` on its first write
of a run, meaning silver is **fully rebuilt from bronze on every
execution** rather than incrementally updated. This is intentionally
simpler than bronze's file-by-file skip logic: silver reads one
complete upstream table and produces one complete downstream table, so
there is no partial-load state to track between runs.

---

## 7. Verification (`verify_silver` task)

The final Airflow task in the silver DAG performs two kinds of check:

**For the four bronze-sourced tables** (`links`, `movies`, `ratings`,
`tags`): confirms `silver_count + quarantine_count == bronze_count`
for each. Any mismatch means a row was silently lost somewhere in the
pipeline and the task raises an error.

**For the two derived tables** (`movie_metadata`,
`user_ratings_master`): confirms the row count is at or above a
sane minimum, since there is no bronze equivalent to compare against
directly.

## 8. Final Verified Row Counts

| Table | Bronze | Silver | Quarantined |
|---|---|---|---|
| `links` | 87,585 | 87,585 | 0 |
| `movies` | 87,585 | 87,585 | 0 |
| `ratings` | 32,000,204 | 32,000,204 | 0 |
| `tags` | 2,000,072 | 2,000,072 | 0 |
| `movie_metadata` | — | 87,585 | — |
| `user_ratings_master` | — | 32,000,204 | — |

## 9. Known Limitations / Scope Decisions

- `silver.imdb_ratings` and `silver.tmdb_ratings` were **not built**.
  These require IMDb/TMDb API integration, which was marked "on hold"
  in the original sprint plan at the bronze layer (`bronze.imdb`,
  `bronze.tmdb`). No downstream gold table depends on this data.
- `(no genres listed)` values are preserved as-is in `Genres` rather
  than converted to null — silver does not decide how gold should
  interpret a category, it only guarantees the value is not corrupted.
