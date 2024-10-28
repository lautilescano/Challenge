# SF Fire Department Analytics

## Solution Overview
This solution implements a modern data pipeline for analyzing San Francisco Fire Department incident data, focusing on efficient querying and analysis capabilities. The architecture follows ELT (Extract, Load, Transform) principles, prioritizing scalability and maintainability.

### Why This Approach?

1. **ELT Over ETL**
   - Raw data preservation in its original form
   - Flexibility to transform data differently as requirements evolve
   - Leverages modern data warehouse computing power
   - Enables easier data lineage tracking and debugging

2. **Technology Choices**
   - **PostgreSQL**: Robust support for geospatial data and complex analytics
   - **dbt**: Modular, version-controlled data transformations
   - **Async Python**: Efficient I/O operations for data extraction and loading
   - **Docker**: Consistent development and deployment environment

3. **Performance Considerations**
   - Incremental processing to handle large datasets efficiently
   - Strategic indexing on frequently queried dimensions
   - Pre aggregated fact tables for common analysis patterns
   - Concurrent index creation to minimize downtime

4. **Data Modeling**
   - Dimensional model optimized for analytical queries
   - Fact tables at different granularities for performance
   - Date dimension for efficient time-based analysis
   - Embedded geographic dimensions for simplified querying

### Key Features
- Daily incremental updates from SF OpenData API
- Response time analysis by district and battalion
- Geographic incident distribution analysis
- Performance monitoring and optimization
- Automated data quality checks

## Architecture

### Data Flow
1. **Extract**: Asynchronous Python extractor pulls data from SF OpenData API
2. **Load**: Raw data loaded into PostgreSQL warehouse
3. **Transform**: dbt models transform raw data into analytics-ready tables

### Key Components
- **API Extractor**: Async Python service using aiohttp
- **Data Warehouse**: PostgreSQL with schema separation (raw, staging, mart)
- **Transformation Layer**: dbt models for dimensional modeling
- **Docker**: Containerized PostgreSQL instance for local development

## Design Decisions & Assumptions

### Data Modeling
1. **Fact Tables**
   - `fact_incidents`: Granular incident data
   - `fact_incident_aggregations`: Pre-aggregated metrics for performance

2. **Dimension Tables**
   - `dim_date`: Time dimension for efficient time-based queries
   - Geographic dimensions embedded in fact tables due to low cardinality

### Incremental Processing
- Daily incremental updates assumed based on requirements
- Implemented using dbt incremental models
- Uses `_loaded_at` timestamp for change tracking

### Performance Optimizations
1. **Indexing Strategy**
   - Indexes on frequently queried dimensions (battalion, district)
   - Date-based partitioning for large fact tables
   - Concurrent index creation to minimize downtime

2. **Pre-aggregation**
   - Common aggregations materialized for faster queries
   - Balanced with storage requirements

## Setup & Usage

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- dbt-core and dbt-postgres

### Installation

## Project Structure

sf_fire_analytics/
├── src/
│ ├── extractors/ # Data extraction from API
│ └── loaders/ # Database loading logic
├── dbt/
│ ├── models/ # Data transformation models
│ │ ├── staging/ # Initial transformations
│ │ └── mart/ # Business-ready models
│ └── analyses/ # Example analytical queries
├── tests/ # Python test files
└── docker/ # Docker configuration

### Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start PostgreSQL:
   ```bash
   docker-compose up -d
   ```
### Running the Pipeline

```bash
python main.py
```
### Example Analytics

1. **Response Time Analysis**
   - Location: `dbt/analyses/response_time_analysis.sql`
   - Analyzes average response times by district and battalion
   - Includes delayed response percentage metrics

2. **District Performance**
   - Location: `dbt/analyses/district_performance.sql`
   - Evaluates incident density and response efficiency by district
   - Provides daily averages and trends

## Project Structure

sf_fire_analytics/
├── src/
│ ├── extractors/ # API data extraction
│ ├── loaders/ # Database loading logic
│ ├── utils/ # Shared utilities
│ └── tests/ # Unit tests
├── dbt/
│ ├── models/ # Data transformation models
│ │ ├── staging/ # Initial cleaning
│ │ └── mart/ # Business-ready models
│ └── analyses/ # Example queries
├── docker/ # Container configurations
└── tests/ # Integration tests

## Testing
- Unit tests: `pytest src/tests`
- Integration tests: `pytest tests`
- dbt tests: `dbt test`

## Monitoring & Maintenance
- Logging using loguru
- Database connection health checks
- Retry mechanisms for API and database operations
