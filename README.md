# MovieLens 32M: End-to-End Data Architecture & Analytics

## Project Overview
This project is an automated, end-to-end data engineering pipeline that processes over 32 million movie ratings and tags into an interactive business intelligence dashboard. 

The goal of this project is to transform raw, unstructured CSV files into clean, reliable, and pre-aggregated datasets. By implementing a **Medallion Architecture**, the pipeline ensures data quality, protects downstream analytical workloads, and provides business stakeholders with lightning-fast insights into user engagement and content performance without requiring them to write SQL.

## Tech Stack
* **Data Processing:** Apache Spark (PySpark)
* **Database / Data Warehouse:** PostgreSQL 16
* **Workflow Orchestration:** Apache Airflow 2.9 (Docker)
* **Business Intelligence:** Tableau Desktop

## Architecture & Data Flow
*(Insert `Movie_Lens_End_to_End_Data_Architecture_Diagram.png` here)*

The data flows through three distinct layers (The Medallion Architecture) to progressively clean and enrich the information:

### 1. :third_place_medal: Bronze Layer (Raw Ingestion)
Raw CSV files (movies, ratings, tags, links) are read by PySpark and loaded directly into PostgreSQL. 
* **Action:** Data is loaded exactly "as-is" to maintain a perfect historical record.
* **Audit:** A `load_tstmp` is appended to every row to track ingestion times.

### 2. :second_place_medal: Silver Layer (Clean & Conformed)
Data is cleaned, filtered, and standardized for enterprise use. 
* **Transformations:** Column names are converted to PascalCase (e.g., `UserId`), timestamps are standardized to UTC, and exact duplicates are removed.
* **Data Quality (Quarantine):** Rows with missing primary keys or invalid metrics (e.g., future timestamps) are isolated and routed to a specific `quarantine` schema. This allows the pipeline to finish successfully without crashing, ensuring business continuity while data engineers investigate the bad records.

### 3. :first_place_medal: Gold Layer (Business Aggregates)
The analytics-ready layer. Instead of forcing Tableau to query 32 million rows directly, this layer pre-calculates the heavy math.
* **Transformations:** Data is aggregated by user, movie, and genre into summary tables (`movie_insight`, `user_insight`, etc.).
* **Benefit:** Provides highly optimized, read-heavy tables specifically designed for fast dashboard rendering.

## Key Engineering Features

* **Idempotency via Staging Upserts:** The Silver layer pipeline is fully idempotent. Instead of blindly appending data (which causes duplicates) or dropping tables (which breaks downstream Gold views), the pipeline uses a Staging Table pattern. It temporarily stages new data, deletes matching old records from the main table using database indexes, and inserts the fresh data. Running the pipeline 100 times yields the exact same accurate result as running it once.
* **Parallel Processing:** To handle 32+ million rows within a 6 GB RAM constraint, the PySpark ingestion uses native JDBC partitioning (`numPartitions=16`). This opens 16 simultaneous database connections, dividing the workload evenly across CPU cores and reducing processing time from over an hour to just minutes.
* **Memory Management:** Strategic use of PySpark's Lazy Evaluation features (`df.cache()` and `df.unpersist()`) prevents redundant data processing and protects the Spark Driver from OutOfMemory (OOM) errors during heavy transformations.

## Tableau Analytics (Business Intelligence)
The final deliverable is a set of interactive Tableau dashboards connected to the Gold layer via a **Data Extract**. 
* **Why Extract over Live?** Because the data pipeline runs in batches, a live connection would waste database compute resources. An extract pulls a highly compressed snapshot of the Gold layer into Tableau's memory, ensuring instant load times and filtering for the end-user.
* **View 1 (Movie & Ratings Insights):** Tracks global metrics like Total Movies, Average Rating, and Top Rated content, sliceable by Genre, Year, and Rating Range.
* **View 2 (User & Engagement):** Highlights the most active users, tagging trends, and day-of-week engagement metrics.