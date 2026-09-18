### View 2 – User, Tags & Engagement

#### KPI Validation

| KPI Requirement | Gold Source | Verified Result | Status |
|---|---|---|---|
| Total Users | `gold.user_insight` | 200,948 users | Available |
| Avg Ratings per User | `gold.user_insight.NumberOfMoviesRated` | Approximately 159.25 ratings per user | Available |
| Most Active User | `gold.user_insight.NumberOfMoviesRated` | UserId 175325 with 33,332 ratings | Available |
| Total Tags | `gold.movie_insight.TotalTags` | 2,000,072 tags | Available |
| Most Tagged Movie | `gold.movie_insight.TotalTags` | Pulp Fiction with 6,697 tags | Available |
| Most Used Tag | Existing Gold tag fields | Overall/global tag frequency is not available directly | Gap |

##### KPI Validation Notes

- `gold.user_insight` contains one record per user and supports Total Users, Average Ratings per User, and Most Active User KPIs.
- `gold.movie_insight` contains movie-level tag counts and supports Total Tags and Most Tagged Movie KPIs.
- Pulp Fiction currently has the highest number of tags with 6,697 tag records.
- Several tag-related fields exist in Gold, such as `PopularTag`, `MostUsedTag`, `TotalTagsUsed`, and `DistinctTags`.
- However, these fields are stored at different aggregation levels such as movie, user, genre, or year.
- The current Gold layer does not contain a global tag-frequency dataset that shows each tag together with its total usage count across the complete dataset.
- A validation query on `silver.tags` shows that `sci-fi` is currently the most frequently used tag with 10,996 occurrences.
- Since the Tableau dashboard is expected to use Gold as its reporting source, the Most Used Tag KPI requires additional Gold-layer support.

#### Filter Validation

| Filter Requirement | Existing Gold Support | Status |
|---|---|---|
| User ID | `UserId` is available in `gold.user_insight` only | Partial |
| Genre | `Genre` is available in `gold.genre_insight` only | Partial |
| Year | `ReleaseYear` is available in `gold.movie_insight` and `Year` in `gold.yearly_insight` | Partial |
| Tag Keyword | No row-level `Tag` field exists in the current Gold layer; only aggregated tag fields are available | Partial |

##### Filter Validation Notes

- The required View 2 filters are not available together in a single Gold dataset.
- `UserId` is available in `gold.user_insight`.
- `Genre` is available in `gold.genre_insight`.
- Year is available as `ReleaseYear` in `gold.movie_insight` and as `Year` in `gold.yearly_insight`.
- No exact row-level `Tag` field is available in the Gold layer.
- Existing tag-related columns such as `PopularTag` and `MostUsedTag` are aggregated attributes and cannot represent the complete set of tag values required for a Tag Keyword filter.
- Because User ID, Genre, Year, and Tag are distributed across different Gold tables, the required filters cannot currently be applied consistently to every worksheet in View 2.

### View 2 — Visual Validation

| Visual | Gold Layer Support | Status | Observation |
|---|---|---|---|
| Top 10 Users by Ratings Count | `gold.user_insight` contains `UserId` and `NumberOfMoviesRated`. | Available | Users can be ranked directly by `NumberOfMoviesRated` to create the required bar chart. |
| User Rating Behaviour (Low / Medium / Heavy raters) | `gold.user_insight.NumberOfMoviesRated` is available for every user. | Partial | The underlying metric required for segmentation is available, but the business thresholds defining Low, Medium and Heavy raters are not specified. The categories can be derived in Tableau once the threshold rules are confirmed. |
| Ratings Activity by Day of Week vs Time of Day | No rating timestamp, day-of-week or hour-level field is available in the current Gold insight tables. | Gap | The heatmap requires rating activity at a time-based grain. The existing Gold tables contain aggregated insights and therefore cannot derive day-of-week and time-of-day activity. |
| Top 10 Movies by Number of Tags | `gold.movie_insight` contains `MovieId`, `MovieTitle` and `TotalTags`. | Available | Movies can be ranked by `TotalTags` to create the required bar chart. |
| Movie → User → Tags → Rating Matrix | Required fields are distributed across different Gold tables and no Gold table contains Movie, User, Tag and individual Rating at the same row-level grain. | Gap | A row-level/detail Gold dataset is required if the matrix must be built exclusively from the Gold layer. |
| Tags Count vs Ratings Count per Movie | `gold.movie_insight` contains `MovieId`, `MovieTitle`, `TotalTags` and `TotalRatings` together. | Available | The existing movie-level Gold table directly supports the required scatter plot. |