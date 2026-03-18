# TMDB Movie ETL Pipeline

An automated, containerized ETL pipeline that extracts enriched movie data from the **TMDB API**, stages it in a local **PostgreSQL** instance, and loads a modeled Star Schema into a **Snowflake** Data Warehouse, orchestrated by **Apache Airflow**.



## Project Overview
This project demonstrates a production-grade data engineering workflow. It handles complex JSON nested data, implements robust data transformations using Pandas, and ensures environment consistency using Docker.

### Key Features
* **Orchestration:** Apache Airflow manages task dependencies and scheduling.
* **Containerization:** The entire orchestration layer is isolated via Docker.
* **Data Modeling:** Transforms raw API responses into a clean Star Schema.
* **Hybrid Cloud:** Bridges local staging (Postgres) with cloud warehousing (Snowflake).

---

## Tech Stack
* **Language:** Python
* **Orchestration:** Apache Airflow (Standalone Docker)
* **Databases:** PostgreSQL (Local Staging), Snowflake (Cloud Warehouse)
* **Tools:** Pandas, SQLAlchemy, Requests, Docker, Snowflake-Connector-Python, Git/GitHub

---

## Architecture & Data Flow
1.  **Extraction:** `TMDBClient` fetches movie details, credits, and financial data from TMDB API.
2.  **Staging:** Raw data is saved as JSON and mirrored into a local **PostgreSQL** table (`tmdb_movies_raw`).
3.  **Transformation:** Pandas cleans the data, handles missing values, and reshapes it into relational tables.
4.  **Loading:** The final dataframes are appended to **Snowflake** using an optimized `write_pandas` method.
5.  **Orchestration**: Airflow triggers the extract task (API to Postgres) followed by the transform_load task (Postgres to Snowflake) to ensure data integrity.

---

## Data Schema (Star Schema)
The data is modeled to support analytical queries:

| Table Name | Type | Description |
| :--- | :--- | :--- |
| **DIM_MOVIES** | Dimension | Movie metadata (title, release date, status, etc.) |
| **DIM_PEOPLE** | Dimension | Combined unique list of Cast and Crew members. |
| **DIM_GENRES** | Dimension | Movie genres (Action, Sci-Fi, etc.) |
| **FACT_FINANCE** | Fact | Financial metrics (budget, revenue, runtime). |
| **FACT_CAST** | Fact | Linking table for actors and their roles in movies. |
| **FACT_CREW** | Fact | Linking table for crew members and their departments. |

---

## Project Structure
```text
TMDB_ETL_Pipeline/
├── dags/               # Airflow DAG definitions
├── src/
│   ├── extract/        # API Client logic
│   ├── transform/      # Pandas transformation logic
│   ├── load/           # Snowflake & Postgres loading scripts
│   └── utils/          # Database & Path utilities
├── data/               # Local temporary JSON file
├── Dockerfile          # Custom Airflow image definition
├── docker-compose.yml  # Container orchestration
└── .env                # Environment variables
```

---

## Getting Started

### Prerequisites
* Docker Desktop
* A TMDB API Key
* A Snowflake Account
* PostgreSQL installed locally

### Installation & Setup
1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Chanpitou/TMDB_ETL_Pipeline.git
    cd TMDB_ETL_Pipeline
    ```
2.  **Configure Environment Variables:**
    Create a `.env` file in the root directory:
    ```env
    API_KEY=****

    POSTGRES_USER=****
    POSTGRES_PASSWORD=****
    #POSTGRES_HOST=****
    POSTGRES_HOST=host.docker.internal
    POSTGRES_PORT=****
    POSTGRES_DB=****
    
    SNOWFLAKE_USER=****
    SNOWFLAKE_PASSWORD=****
    SNOWFLAKE_ACCOUNT=****
    SNOWFLAKE_WAREHOUSE=****
    SNOWFLAKE_DATABASE=****
    SNOWFLAKE_SCHEMA=****
    SNOWFLAKE_ROLE=****
    ```
3.  **Launch the Pipeline:**
    ```bash
    docker-compose up --build -d
    ```
4.  **Access Airflow:**
    Navigate to `http://localhost:8080` (Default: `admin` / password found in `standalone_admin_password.txt`).

---

## Future Improvements
* Implement **S3 Storage** as a data lake instead of local JSON storage.
* Expand raw data extracting from endpoints such as TV_Shows
---