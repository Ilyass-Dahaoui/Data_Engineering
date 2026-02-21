# Setup Guide for Lab 1 - Python Data Pipeline

## Prerequisites
- Python 3.7 or higher
- Virtual environment tool (venv, conda, or virtualenv)

## Environment Setup

### Option 1: Using venv (Built-in Python)
```powershell
python -m venv venv

.\venv\Scripts\Activate.ps1

pip install -r requirements.txt
```



## Project Structure
```
Data_Engineering-main/
├── DATA/
│   ├── raw/                    # Raw data from extraction
│   │   ├── apps_metadata.json
│   │   ├── apps_reviews.json
│   │   └── extract_data.py
│   └── processed/              # Processed/cleaned data
│       ├── apps_metadata_clean.json
│       ├── apps_reviews_clean.json
│       └── apps_with_metrics.json
├── src/                        # Pipeline logic
│   ├── config.py              # Configuration and paths
│   ├── ingest.py              # Data ingestion
│   ├── transform.py           # Data transformation
│   ├── load.py                # Data loading/saving
│   └── pipeline.py            # Main pipeline orchestrator
├── requirements.txt
└── README.md
```

## Running the Pipeline

### Step 1: Extract Raw Data (Optional - data already provided)
```powershell
cd DATA\raw

python extract_data.py
```

### Step 2: Run the Data Pipeline
```powershell
cd src

python pipeline.py
```

### Step 3: Verify Results
Check the `DATA/processed/` directory for output files:
- `apps_metadata_clean.json` - Cleaned app metadata
- `apps_reviews_clean.json` - Cleaned reviews
- `apps_with_metrics.json` - Apps with aggregated review metrics

## Testing Individual Modules
Each module can be tested independently:

```powershell
cd src

python ingest.py

python transform.py

python load.py
```

