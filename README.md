
# Media Analytics ETL Pipeline

This repository contains a complete data pipeline for processing and analyzing user interaction data from an online media company. The solution extracts data from JSON files, transforms it into a structured format, and loads it into a PostgreSQL database for analysis.

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [ETL Pipeline](#etl-pipeline)
- [Schema Design](#schema-design)
- [Data Processing](#data-processing)
- [Sample Queries](#sample-queries)
- [Assumptions](#assumptions)

## Overview

This data pipeline processes user interaction data (page views, article reads, video plays) collected from a news website to provide insights into audience behavior. The solution includes:

1. A Python ETL script to extract, transform, and load JSON data
2. An optimized PostgreSQL database schema
3. Data processing queries for analytics
4. Performance optimization strategies
5. Sample data generator for testing

## Requirements

- Python 3.8+
- PostgreSQL 12+
- Python Packages:
  - pandas
  - psycopg2
  - sqlalchemy
  - faker (for sample data generation)

## Project Structure

```
media-analytics/
├── data/                    # Directory for JSON data files
├── scripts/
│   ├── etl_pipeline.py      # Main ETL script
│   ├── generate_sample_data.py  # Sample data generator
│   ├── db_schema.sql        # Database schema creation script
│   ├── analysis_queries.sql # Analytics queries
│   └── performance_optimization.sql # Performance optimization
├── README.md                # This file
├── requirements.txt         # Python dependencies
└── .env.example             # Example environment variables
```

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/media-analytics.git
cd media-analytics
```

### 2. Set up the Python environment

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Set up the PostgreSQL database

```bash
# Create the database
createdb media_analytics

# Create the schema
psql -d media_analytics -f scripts/db_schema.sql
```

### 4. Configure environment variables

```bash
# Copy the example .env file
cp .env.example .env

# Edit the .env file with your database credentials
```

### 5. Generate sample data (optional)

```bash
python scripts/generate_sample_data.py
```

### 6. Run the ETL pipeline

```bash
python scripts/etl_pipeline.py
```

## ETL Pipeline

The ETL pipeline consists of the following steps:

### Extract

- Reads JSON files from the data directory
- Handles both array-format and line-delimited JSON
- Supports processing multiple files in batches

### Transform

- Converts timestamps to datetime format
- Extracts date/time components for analysis
- Parses URLs to extract content categories and article IDs
- Categorizes referrers by source type
- Generates unique interaction IDs
- Handles missing values and ensures consistent data types

### Load

- Stores processed data in PostgreSQL database
- Uses efficient bulk loading with SQLAlchemy
- Implements error handling and transaction management
- Supports incremental loading

## Schema Design

The database schema follows a dimensional model:

### Fact Table

- `user_interactions`: Main fact table storing all user interaction events

### Dimension Tables

- `dim_users`: User information and aggregated metrics
- `dim_content`: Content metadata and performance metrics
- `dim_sessions`: Session information and metrics
- `dim_date`: Date dimension for time-based analysis
- `dim_time`: Time-of-day dimension for temporal analysis

### Aggregate Tables

- `agg_daily_users`: Daily user activity metrics
- `agg_article_performance`: Article performance metrics

### Key Features

- Table partitioning by date for efficient querying
- Optimized indexes for common query patterns
- Foreign key relationships for data integrity
- Time-series partitioning for high-volume data

## Data Processing

The solution includes queries for:

- Calculating unique visitors per day
- Identifying the most popular articles
- Analyzing engagement by content category
- Tracking traffic sources
- Performing temporal analysis (hourly, daily, monthly trends)
- User retention analysis
- Session analysis
- Content journey analysis

## Performance Optimization

The following optimization strategies are implemented:

- Table partitioning by date
- Strategic indexing on frequently queried columns
- Materialized views for common reports
- Query optimization techniques
- Data retention policies
- Statistics collection for query planner

## Sample Queries

See `scripts/analysis_queries.sql` for a comprehensive set of analytics queries, including:

- Daily visitor counts
- Popular content identification
- Traffic source analysis
- Time-based engagement patterns
- User retention metrics
- Content category performance

## Assumptions

1. Data Format:
   - JSON files contain user interaction data with specified fields
   - Each record has a unique combination of user_id, timestamp, and session_id

2. Data Volume:
   - Solution is designed for moderate to high volume (millions of interactions)
   - Partitioning strategy assumes date-based access patterns

3. Performance Requirements:
   - Analytics queries need to be optimized for daily/weekly reporting
   - Real-time analysis is not required for the initial implementation

4. Security:
   - Basic security measures are implemented
   - User authentication and authorization would be handled separately

5. Data Retention:
   - Detailed data is kept for 1 year
   - Aggregated data is kept indefinitely
