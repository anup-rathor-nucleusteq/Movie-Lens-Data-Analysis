**US-6 — View 1 Gold Layer Gap Analysis**

**Movie & Ratings Insights**

# **1\. Purpose**

This document checks the View 1 dashboard requirements against the Gold Layer tables that already exist. It clearly shows what we can use now and what data is missing.

# **2\. Quick Summary**

| Category | Result |
| :---- | :---- |
| Already available | Most KPIs and visuals can be supported by the current Gold tables. |
| Partially available | Ratings distribution, ratings-over-time, and rating-range filtering need additional data/granularity. |
| Clearly missing | Monthly ratings trend and a proper global ratings distribution are not directly available in the current Gold layer. |

# **3\. Gold Tables We Have**

| Gold Table | Level | Important Fields Available |
| :---- | :---- | :---- |
| gold.movie\_insight | Movie | MovieId, ReleaseYear, MovieTitle, AvgRating, HighestRating, LowestRating, TotalRatings, TotalTags |
| gold.genre\_insight | Genre | Genre, MovieCount, TotalRatings, AvgRating, Rated0\_0 to Rated5\_0, PopularTag |
| gold.yearly\_insight | Year | Year, MoviesReleased, TotalRatings, MostPopularMovie, HighlyRatedMovie, HighestAvgRating, LowestRating, TopRatedGenre, LeastRatedGenre, DistinctTagsUsed, MostPopularTag |

# **4\. View 1 — What We Have vs What Is Missing**

| Dashboard Requirement | What We Have | Status | Simple Explanation |
| :---- | :---- | :---- | :---- |
| Total Movies | movie\_insight → MovieId | AVAILABLE | We have MovieId, so the number of movies can be counted. |
| Distinct Genres | genre\_insight → Genre | AVAILABLE | We have one row for each genre, so distinct genres can be counted. |
| Total Ratings | movie\_insight → TotalRatings | AVAILABLE | We have the total number of ratings for each movie, so they can be added together. |
| Average Rating (Overall) | movie\_insight → AvgRating \+ TotalRatings | AVAILABLE | We have movie average ratings and rating counts. The overall average can be calculated using the rating counts. |
| Top Rated Movie | movie\_insight → MovieTitle, AvgRating, TotalRatings | AVAILABLE | We have the movie name, average rating, and number of ratings needed to show the top-rated movie. |
| Most Rated Genre | genre\_insight → Genre, TotalRatings | AVAILABLE | We have total ratings for each genre, so the most-rated genre can be found. |
| Average Rating by Genre | genre\_insight → Genre, AvgRating | AVAILABLE | We have average rating for every genre. |
| Movies Distribution by Genre | genre\_insight → Genre, MovieCount | AVAILABLE | We have the number of movies for each genre, which can be used for the treemap. |
| Movie Releases over Time | yearly\_insight → Year, MoviesReleased | AVAILABLE | We have the number of movies released in each year. |
| Ratings Distribution (1–5) | genre\_insight → Rated0\_0 to Rated5\_0 | PARTIAL | Rating buckets exist, but they are stored by genre. They cannot simply be added together to create one global rating distribution because a movie can have multiple genres. |
| Top 10 Movies by Number of Ratings | movie\_insight → MovieTitle, TotalRatings | AVAILABLE | We can sort movies by TotalRatings and take the top 10\. |
| Ratings Count vs Average Rating | movie\_insight → TotalRatings, AvgRating | AVAILABLE | We have both values needed for the scatter plot. |
| Ratings Submitted Over Time — Yearly | yearly\_insight → Year, TotalRatings | AVAILABLE | We can show the yearly ratings trend. |
| Ratings Submitted Over Time — Monthly | No month/date-level field in yearly\_insight | MISSING | The current Gold table only stores yearly totals. It does not contain monthly rating totals. |
| Genre Slicer | genre\_insight → Genre | AVAILABLE | Genre values are available for filtering. |
| Year Slicer | yearly\_insight → Year; movie\_insight → ReleaseYear | AVAILABLE | Year information is available for filtering. |
| Rating Range Slicer | movie\_insight → AvgRating, LowestRating, HighestRating | PARTIAL | Rating values exist, but the current Gold layer does not provide a clear row-level rating dataset for flexible rating-range filtering. |

# **5\. Gaps — In Very Simple Language** 

**1\. Monthly Ratings Data is Missing**  
We have ratings grouped by year in gold.yearly\_insight, but we do not have ratings grouped by month. Therefore, the monthly Ratings Submitted Over Time chart cannot be properly created from the current Gold layer.

**2\. Global Ratings Distribution is Missing**  
We have rating buckets from 0 to 5 in gold.genre\_insight, but those buckets are separated by genre. A movie can belong to more than one genre, so adding all genre buckets can count the same rating more than once. Therefore, a correct single overall rating distribution is not directly available.

**3\. Rating Range Filtering is Limited**  
The movie table contains AvgRating, LowestRating, and HighestRating, but it does not contain one row for every individual rating. This limits how a flexible 1–5 rating-range filter can be built.

# **6\. What We Can Build with the Current Gold Layer**

* Total Movies KPI  
* Distinct Genres KPI  
* Total Ratings KPI  
* Overall Average Rating KPI  
* Top Rated Movie KPI  
* Most Rated Genre KPI  
* Average Rating by Genre bar chart  
* Movies Distribution by Genre treemap  
* Movie Releases over Time yearly line chart  
* Top 10 Movies by Number of Ratings bar chart  
* Ratings Count vs Average Rating scatter plot  
* Yearly Ratings Submitted trend  
* Genre slicer  
* Year slicer

# **7\. What the Current Gold Layer Cannot Directly Provide**

* Monthly Ratings Submitted Over Time  
* A correct global Ratings Distribution (1–5) from the existing genre-level rating buckets  
* A fully flexible individual-rating-based Rating Range filter

# **8\. Final View 1 Status**

**The current Gold Layer covers most of View 1\.** The main missing data is month-level rating information and a proper global rating-distribution dataset. The existing Gold tables are sufficient for the majority of the required KPIs and charts.