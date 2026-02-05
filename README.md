# Data Engineering Lab 1 - Python Data Pipeline

This project implements an end-to-end Python-only data pipeline for analyzing AI note-taking mobile apps from the Google Play Store. The pipeline transforms raw data through ingestion, transformation, and loading stages to create analytics-ready outputs.

## Project Overview

**Objective**: Build a data pipeline that processes app metadata and user reviews for competitive market research.

**Data Sources**:
- **Apps Catalog** (Dimension data): Metadata about AI note-taking apps
- **Apps Reviews** (Fact data): User reviews from app stores

**Pipeline Stages**:
1. **Generation/Extraction**: Scrape data from Google Play Store
2. **Ingestion**: Read raw JSON data files
3. **Transformation**: Clean, standardize, and aggregate data
4. **Loading**: Save processed data for analytics

## Project Structure

```
Data_Engineering-main/
├── DATA/
│   ├── raw/                    # Stage: Generation/Ingestion
│   │   ├── apps_metadata.json  # App catalog data
│   │   ├── apps_reviews.json   # User reviews (JSONL format)
│   │   └── extract_data.py     # Data extraction script
│   └── processed/              # Stage: Loading
│       ├── apps_metadata_clean.json
│       ├── apps_reviews_clean.json
│       └── apps_with_metrics.json
├── src/                        # Pipeline logic
│   ├── config.py              # Configuration and paths
│   ├── ingest.py              # Data ingestion module
│   ├── transform.py           # Data transformation module
│   ├── load.py                # Data loading module
│   └── pipeline.py            # Main orchestrator
├── requirements.txt
├── SETUP.md                   # Detailed setup instructions
└── README.md
```

## Quick Start

### 1. Setup Environment
```powershell
# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2. Run the Pipeline
```powershell
cd src
python pipeline.py
```

### 3. View Results
Processed files will be in `DATA/processed/`:
- `apps_metadata_clean.json` - Cleaned app metadata
- `apps_reviews_clean.json` - Cleaned and standardized reviews  
- `apps_with_metrics.json` - Apps with aggregated review metrics (rating distribution, reply rates, etc.)

## Key Features

###  Feedback Implemented
- **Continuation Tokens**: Reviews extraction uses pagination tokens to fetch maximum available data
- **Append Mode**: Reviews are written incrementally to prevent data loss if extraction crashes
- **Robust Error Handling**: Pipeline continues even if individual apps fail
- **Flexible JSON Loading**: Supports both JSON array and JSONL formats

### Pipeline Capabilities
- **Data Quality**: Handles missing values, type conversions, date parsing
- **Standardization**: Normalizes field names and data formats
- **Aggregation**: Calculates review metrics (avg score, rating distribution, reply rates)
- **Joins**: Combines dimension (apps) and fact (reviews) data
- **Modular Design**: Each stage can be tested independently

## Module Overview

### `extract_data.py`
- Scrapes Google Play Store for AI note-taking apps
- Uses continuation tokens for complete data extraction
- Appends reviews incrementally to prevent data loss

### `ingest.py`
- Loads raw JSON data (supports JSON array and JSONL)
- Provides functions to read apps metadata and reviews

### `transform.py`
- Cleans and standardizes data fields
- Handles missing values and type conversions
- Aggregates reviews by app (metrics: avg score, rating distribution, reply rate)
- Joins apps with review metrics

### `load.py`
- Saves processed data to JSON files
- Provides functions to load processed data

### `pipeline.py`
- Orchestrates the complete ETL pipeline
- Provides detailed logging and error reporting
- Tracks execution time and data volumes

## Development

### Testing Individual Modules
```powershell
cd src

# Test ingestion
python ingest.py

# Test transformation
python transform.py
```

### Adding New Features
1. Add new transformations in `transform.py`
2. Update configuration in `config.py`
3. Run the pipeline to regenerate processed data

## Troubleshooting

See [SETUP.md](SETUP.md) for detailed setup instructions and troubleshooting guide.

## License

See [LICENSE](LICENSE) file for details.

---

**Course**: Data Engineering Labs  
**Lab**: Lab 1 - Python-only Data Pipeline  
**Date**: January 2026
