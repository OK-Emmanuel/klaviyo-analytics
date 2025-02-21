Revenue Attribution Analysis Tool for Klaviyo
This project analyzes revenue attribution from Klaviyo campaign and flow data over a 365-day period. It retrieves data via the Klaviyo API, performs attribution calculations, and outputs results to JSON and CSV files.

Installation
This project requires Python 3.7 or higher. We recommend using a virtual environment for dependency management.

1. Create a virtual environment:

```
python3 -m venv .venv  # Creates a virtual environment named '.venv'
source .venv/bin/activate  # Activates the virtual environment (Linux/macOS)
.venv\Scripts\activate  # Activates the virtual environment (Windows)
```

2. Install requirements:

```
pip install -r requirements.txt
```
This will install all necessary Python packages listed in requirements.txt. 

3. Configure Klaviyo API Credentials:

Before running the script, you'll need to configure your Klaviyo API key. Your Private API Key should be placed in .env file

After completing these steps, you should be able to run the main script to retrieve, analyze, and output revenue attribution data.