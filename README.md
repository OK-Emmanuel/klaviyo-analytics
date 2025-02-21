# Revenue Attribution Analysis Tool for Klaviyo

This project analyzes revenue attribution from Klaviyo campaign and flow data over a 365-day period. It retrieves data via the Klaviyo API, performs attribution calculations, and outputs results to Pandas DataFrames, JSON, and CSV files. Three features are implemented, available in both CLI and a unified Streamlit app:

- **Feature 1: Revenue Attribution Split**
- **Feature 2: Product Purchase Attribution**
- **Feature 3: Klaviyo Attribution Share**

## Features

### Feature 1: Revenue Attribution Split
- Fetches Klaviyo campaigns and flows from the past 365 days.
- Splits revenue into new and recurring customer categories.
- Outputs: `revenue_attribution_results.json`, `revenue_attribution_results.csv`.

### Feature 2: Product Purchase Attribution
- Tracks product purchases attributed to campaigns and flows.
- Includes product details: ID, name, units sold, type, and revenue.
- Handles deduplication and aggregates data daily.
- Outputs: `product_attribution_results.json`, `product_attribution_results.csv`.

### Feature 3: Klaviyo Attribution Share
- Calculates daily Klaviyo revenue share as a percentage of total shop revenue.
- Aggregates "Placed Order" events by day, distinguishing attributed vs. total revenue.
- Outputs: `revenue_share_results.json`, `revenue_share_results.csv`.

## Installation

This project requires **Python 3.7 or higher**. Use a virtual environment for dependency management.

1. **Create a virtual environment**:
## Installation

This project requires **Python 3.7 or higher**. We recommend using a virtual environment for dependency management.

1. **Create a virtual environment**:

python3 -m venv .venv  # Creates a virtual environment named '.venv'
source .venv/bin/activate  # Activates the virtual environment (Linux/macOS)
.venv\Scripts\activate  # Activates the virtual environment (Windows)


2. **Install requirements**:
pip install -r requirements.txt

This will install all necessary Python packages listed in `requirements.txt`. For the web version (`app.py`), ensure `streamlit` is included in your `requirements.txt`. A sample `requirements.txt` might look like:
requests
pandas
python-dotenv
streamlit  # Required for app.py


3. **Configure Klaviyo API Credentials**:
Before running either script (CLI or Webview), configure your Klaviyo API keys:
- Create a `.env` file in the project root.
- Add your Private API Key (Klaviyo API Key) and Public API Key:

KLAVIYO_API_KEY=your_private_api_key_here
PUBLIC_API_KEY=your_public_api_key_here


- For the Streamlit Webview app, input the key directly in the UI or use `.env`.

## Usage

### CLI Versions
- **Revenue Attribution Split (`revenue.py`)**:

python revenue.py
- **Product Purchase Attribution (`products.py`)**:

python products.py
- **Klaviyo Attribution Share (`share.py`)**:

python share.py

### Unified Streamlit App (`app.py`)
- **Run with**:
streamlit run app.py

- **Description**: Launches a web interface with tabs for all three features. Enter your API key in the sidebar, click "Run All Analyses," and view/download results for:
- Revenue Attribution Split
- Product Purchase Attribution
- Klaviyo Attribution Share
- **Outputs**: Saves results as `revenue_attribution_results.{json,csv}`, `product_attribution_results.{json,csv}`, and `revenue_share_results.{json,csv}`.

## Requirements

- `requests`: API calls.
- `pandas`: Data processing.
- `python-dotenv`: Load `.env` (CLI only).
- `streamlit`: Web interface (app.py only).

## Notes

- Uses Klaviyo API revision "2025-01-15".
- Handles rate limits with retries.
- Processes data in batches with robust pagination.
- Includes error handling and data validation.
- For Feature 3, total shop revenue is derived from all "Placed Order" events in Klaviyo; adjust if an external source is available.