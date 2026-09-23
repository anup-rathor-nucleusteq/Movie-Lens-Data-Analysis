# Gold Layer — Process & Dataflow Documentation

**Project:** MovieLens 32M Data Engineering Capstone
**Layer:** Gold (business-ready insight tables)
**Source:** `silver.user_ratings_master`, `silver.tags`, `silver.movies`, `silver.movie_metadata`
**Orchestration:** Airflow DAG `gold_build`
**Trigger:** Automatic, via Airflow Dataset `SILVER_READY` (fires when `silver_transform` DAG completes successfully)

---

## 1. Purpose

Silver produces clean, correct, row-level data — but 32 million rows
is not something a person or a dashboard queries directly. Gold
answers specific business questions by pre-aggregating silver into
four small summary tables, one row per entity of interest (movie,
user, genre, release year). This is what Power BI / Tableau connects
to: importing 87,585 rows is instant, importing 32 million is not.

---

## 2. Codebase Layout

| File | Role |
|---|---|
| `src/python/gold_helpers.py` | Three reusable aggregation patterns shared across all four tables |
| `src/pyspark/build_gold.py` | The build job — one function per table |
| `airflow_dags/dag_gold_build.py` | Airflow DAG wiring the build functions into tasks |
| `sql/gold_queries.sql` | Equivalent logic expressed as plain SQL, for review independent of the Spark code |

---

## 3. Reusable Aggregation Helpers (`gold_helpers.py`)

Every gold table repeatedly needs to answer one of three shapes of
question. Rather than writing bespoke logic four times, three
functions cover every case:

| Function | Question it answers | How |
|---|---|---|
| `mode_of` | "What is the most common value in this group?" (e.g. most popular tag) | Counts occurrences per group, ranks by frequency (ties broken alphabetically for determinism), keeps rank 1 |
| `top_row_per_group` / `best_row_per_group` | "Which single row is the best in this group, by some sort order?" (e.g. most-rated movie in a genre) | `row_number()` over a window partitioned by the group, ordered by the chosen sort columns; keeps rank 1 |
| `top_n_array` / `top_n_list` | "What are the top N rows in this group, as a list?" (e.g. top 10 movies per genre — required as an Array type) | Same ranking approach, but keeps ranks 1 through N and `collect_list`s them |
| `add_gold_audit` | Adds `LoadTs` and `UpdateTs` in UTC | `current_timestamp()` on both columns |

---

## 4. Reading Strategy

Two different read patterns are used depending on what a table groups
by:

- **`read_master_full` / `read_master`** — reads all 32M rows of
  `silver.user_ratings_master` split across 8–16 parallel JDBC
  connections by `UserId` range. Used whenever the table groups by
  something *other than* `UserId` (`MovieId`, `Genre`, `ReleaseYear`),
  because in those cases one movie's or genre's ratings are scattered
  across every user and cannot be isolated by a user-range filter.

- **Batched reads by `UserId` range** — used for `user_insight`,
  which groups by `UserId` directly, so each batch of ~20,000 users
  can be processed and written independently without ever holding the
  full table in memory at once.

---

## 5. Table-by-Table Dataflow

### 5.1 `gold.movie_insight`

```
silver.user_ratings_master (full parallel read)
  → select MovieId, Title, ReleaseYear, Rating
  → groupBy MovieId:
       AvgRating, HighestRating, LowestRating, TotalRatings

silver.tags (full read)
  → groupBy MovieId:
       TotalTaggers (distinct UserId), TotalTags, DistinctTags
  → mode_of(MovieId, TagText) → PopularTag

rating_stats LEFT JOIN tag_stats LEFT JOIN popular_tag  ON MovieId
  → fillna 0 for movies with no tags
  → add LoadTs, UpdateTs
  → write gold.movie_insight (overwrite)
```

**87,585 rows** — one per movie, matching `silver.movies` exactly.
Tag statistics are sourced from `silver.tags` directly rather than
from `user_ratings_master`, because a user can tag a movie without
ever rating it — that activity would be invisible if tags were only
read from the ratings-joined master table.

### 5.2 `gold.user_insight`

```
For each batch of 20,000 users:
  silver.user_ratings_master (batch)
    → groupBy UserId:
         MoviesRated, AvgRatingLifetime, LeastRatingGiven,
         HighestRatingGiven, FiveStarCount, AvgRatingsPerYear
         (= total ratings / distinct active years, not calendar
           years — a user who rated 100 films in one year and then
           stopped averaged 100/year, not 100 divided by every year
           since)

    → explode Genres (split on "|")
    → groupBy (UserId, Genre): count, avg rating
    → top_row_per_group → TopGenre (most-rated genre)
    → top_row_per_group → BestRatedGenre (highest-average genre)
    → top_row_per_group on RatingTstmp desc → MostRecentRatedMovie

  silver.tags (same batch of users)
    → groupBy UserId: TotalTagsUsed
    → mode_of(UserId, TagText) → MostUsedTag
    → join to silver.movies for title
    → top_row_per_group on TagTstmp desc → MostRecentTaggedMovie

  join all pieces on UserId
  → fillna 0 for TotalTagsUsed
  → add LoadTs, UpdateTs
  → write gold.user_insight (batch 1 = overwrite, batches 2+ = append)
```

**200,948 rows** — one per user, matching the dataset's documented
user count exactly.

### 5.3 `gold.genre_insight`

The most computationally demanding table: ~32M rows explode to
roughly 80M once genres are split, and the table answers six distinct
"which movie is the X in this genre" questions plus two top-10 arrays.

```
silver.user_ratings_master (full parallel read)
  → explode Genres (split on "|")
  → cache (reused for multiple aggregations below)

  → groupBy Genre:
       MovieCount, TotalRatings, AvgRating,
       + one bucketed count column per half-star value (0.5–5.0)

  → groupBy (Genre, MovieId):
       Title, ReleaseYear, RatingCount, MovieAvg
    → cache, materialize once

    -- Four ranking questions computed in a SINGLE combined pass
    -- (one shuffle), rather than four/six independent window
    -- operations each re-shuffling the same data:
    → row_number() by RatingCount desc  → rank "_rn_most"
    → row_number() by RatingCount asc   → rank "_rn_least"
    → row_number() by MovieAvg desc     → rank "_rn_best"
    → row_number() by MovieAvg asc,
                    ReleaseYear desc    → rank "_rn_worst"

    → filter _rn_most  == 1               → MostRatedMovie
    → filter _rn_least == 1               → LeastRatedMovie
    → filter _rn_best  == 1 AND
             RatingCount >= 100           → HighestAvgRatedMovie
             (the 100-rating minimum prevents a film with one
              5-star review from winning this category)
    → filter _rn_worst == 1               → WorstRatedMovie
    → filter _rn_most  <= 10, collect_list → Top10PopularMovies
    → filter _rn_best  <= 10 AND
             RatingCount >= 100, collect_list → Top10RatedMovies

silver.tags JOIN (silver.movies exploded by Genre) ON MovieId
  → mode_of(Genre, TagText) → PopularTag

join every piece above on Genre
  → add LoadTs, UpdateTs
  → write gold.genre_insight (overwrite)
```

**~19–20 rows** — one per distinct genre value in the dataset
(including the literal `(no genres listed)` category, preserved as-is
from silver rather than converted to null).

**Performance note:** an earlier version of this function called the
ranking helper six separate times, each triggering its own full
shuffle of the ~80M-row exploded dataset — this caused the task to run
for multiple hours and eventually be killed by Docker's memory
limiter. The fix consolidates all four ranking questions into one
combined pass with multiple `row_number()` columns computed together,
so the expensive shuffle happens once instead of six times. Combined
with a fix to `write_to_postgres()` (see §6), task runtime dropped
from 5+ hours (failing) to a few minutes (succeeding).

### 5.4 `gold.yearly_insight`

```
silver.user_ratings_master (full parallel read)
  → filter ReleaseYear IS NOT NULL
    (movies with no year in the source title cannot be attributed
     to a release year and are excluded from this table only)
  → cache

  → groupBy ReleaseYear:
       MoviesReleased, TotalRatingsGiven

  → groupBy (ReleaseYear, MovieId):
       Title, RatingCount, MovieAvg, MovieMin
    → cache
    → top_row_per_group → MostPopularMovie (highest RatingCount)
    → top_row_per_group (RatingCount >= 100) → HighlyRatedMovie
    → groupBy ReleaseYear: max(MovieAvg), min(MovieMin)
                          → HighestAvgRating, LowestRatingGiven
    → top_row_per_group (lowest avg) → WorstRatedMovie

  → explode Genres
  → groupBy (ReleaseYear, Genre): avg rating
    → top_row_per_group (highest avg) → TopRatedGenre
    → top_row_per_group (lowest avg)  → LeastRatedGenre

  silver.tags JOIN silver.movie_metadata ON MovieId
    → filter ReleaseYear IS NOT NULL
    → groupBy ReleaseYear: DistinctTagsUsed
    → mode_of(ReleaseYear, TagText) → MostPopularTag

  join every piece above on ReleaseYear
  → fillna 0 for DistinctTagsUsed
  → rename ReleaseYear → Year
  → add LoadTs, UpdateTs
  → write gold.yearly_insight (overwrite)
```

**~100–135 rows** — one per release year present in the dataset.
"Year" here always means the movie's *release* year (parsed from the
title in silver), never the year a rating was submitted — this
distinction matches the specification's wording ("number of movies
released in the year").

---

## 6. Performance Fixes Applied During Development

Two changes were required to get the gold layer running reliably
within the project's local (Docker Desktop, single laptop) resource
constraints:

**`write_to_postgres()` (in `spark_utils.py`)** originally called
`df.count()` immediately before `df.write.jdbc(...)`. Because Spark
is lazily evaluated, this meant every join/explode/aggregation chain
in a table's build was computed twice — once to produce the count,
once again to produce the write. The fix adds `df.cache()` before the
count, so both the count and the write read from the same
already-computed result instead of recomputing it from scratch. This
change benefits every table in both the silver and gold layers, since
all of them call this same function.

**`build_genre_insight()`** originally called the ranking helper
(`top_row_per_group` / `top_n_array`) six separate times against the
same ~80M-row per-movie table, each call performing its own
independent shuffle. This was consolidated into a single combined
window computation (see §5.3) producing four `row_number()` ranks in
one pass, then filtering that one result six different ways. Filtering
a cached result is inexpensive; re-shuffling 80M rows six times was
the dominant cost in the task's multi-hour runtime.

Docker Desktop's memory allocation was also increased (Settings →
Resources → Memory) to give the Spark driver sufficient headroom
alongside the Airflow scheduler process running in the same container.

---

## 7. Verification (`verify_gold` task)

The final Airflow task confirms every gold table has at least a
sane minimum row count (e.g. `movie_insight >= 80,000`). An empty or
near-empty table — which would typically indicate a join silently
produced no matches — fails the task immediately rather than being
discovered later as a blank chart in the BI dashboard.

## 8. Final Verified Row Counts

| Table | Rows | Grain |
|---|---|---|
| `gold.movie_insight` | 87,585 | one row per movie |
| `gold.user_insight` | 200,948 | one row per user |
| `gold.genre_insight` | ~19–20 | one row per genre |
| `gold.yearly_insight` | ~100–135 | one row per release year |

`movie_insight`'s row count was cross-checked against
`silver.movies` to confirm no movies were dropped by an inner-join
mistake; the two counts match exactly.

## 9. Downstream Consumption

All four gold tables are designed to be connected to directly by the
BI tool (Power BI / Tableau) via the native PostgreSQL connector,
using an **import/extract** connection rather than a live/DirectQuery
connection — table sizes here (under 250,000 rows each) make a full
import fast, and it avoids repeatedly querying Postgres on every
dashboard interaction.

Two visuals in the dashboard specification (a day-of-week/time-of-day
activity heatmap, and a Movie→User→Tag→Rating detail matrix) require
row-level transaction data that these four aggregated tables do not
retain. This is a known scope gap between the gold layer as built and
the full dashboard specification — see the project's visualization
documentation for how it is addressed.
