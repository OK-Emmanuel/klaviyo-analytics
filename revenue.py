import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
import time
import json
import pandas as pd

load_dotenv()

# Configuration
KLAVIYO_API_KEY = os.getenv("KLAVIYO_API_KEY")
if not KLAVIYO_API_KEY:
    raise ValueError("No API key found. Please create a .env file with your KLAVIYO_API_KEY")

KLAVIYO_API_URL = "https://a.klaviyo.com/api"

print(f"Loaded API Key: {KLAVIYO_API_KEY}")  # Ensure it's not None


def make_klaviyo_request(endpoint, params=None):
    """
    Make a request to Klaviyo API with enhanced error handling
    """
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
        "Accept": "application/json",
        "revision": "2025-01-15"
    }

    # Remove any leading slash from endpoint
    endpoint = endpoint.lstrip('/')
    url = f"{KLAVIYO_API_URL}/{endpoint}"
    
    print(f"Making request to: {url}")  # Debug print
    print(f"With params: {params}")     # Debug print
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        print(f"Response for {endpoint}: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")  # Debug print
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limit reached. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            return make_klaviyo_request(endpoint, params)
        
        # Log all non-200 responses
        if response.status_code != 200:
            print(f"Error response: {response.text}")
            return None
            
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"API Request failed for endpoint {endpoint}: {str(e)}")
        return None

def get_campaigns_and_flows():
    """
    Fetch both campaigns and flows with minimal parameters
    """
    # Initialize lists to store campaigns and flows
    campaign_list = []
    flow_list = []
    
    # Fetch email campaigns with pagination
    next_page = None
    while True:
        if next_page is None:
            params = {"filter": "equals(messages.channel,'email')"}
        else:
            params = {"page[cursor]": next_page, "filter": "equals(messages.channel,'email')"}
        
        campaigns = make_klaviyo_request("campaigns", params=params)
        
        if campaigns and 'data' in campaigns:
            campaign_list.extend(campaigns['data'])
            if 'links' in campaigns and 'next' in campaigns['links'] and campaigns['links']['next']:
                next_page = campaigns['links']['next'].split("?page[cursor]=")[1]
            else:
                break
    
    # Fetch flows with minimal parameters
    flows = make_klaviyo_request("flows")
    
    if flows and 'data' in flows:
        flow_list = flows['data']
    
    return campaign_list, flow_list

def process_revenue_attribution(campaigns, flows):
    """
    Process revenue attribution for campaigns and flows
    Returns a pandas DataFrame
    """
    results = []
    
    # Process campaigns
    for campaign in campaigns:
        try:
            # Extract campaign data based on API version
            campaign_id = campaign.get('id', campaign.get('campaign_id', ''))
            if not campaign_id:
                continue
            
            # For now, just collect basic campaign info
            results.append({
                "campaign_id": campaign_id,
                "campaign_name": campaign.get('name', campaign.get('campaign_name', 'Unknown')),
                "send_time": campaign.get('created', campaign.get('created_at', datetime.utcnow().isoformat())),
                "total_attributed_revenue": 0,  # Placeholder
                "new_customers_revenue": 0,     # Placeholder
                "recurring_customers_revenue": 0 # Placeholder
            })
            
        except Exception as e:
            print(f"Error processing campaign: {str(e)}")
            continue
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(results)
    
    # Save both JSON and CSV formats if we have data
    if not df.empty:
        df.to_json("revenue_attribution_results.json", orient="records", indent=2)
        df.to_csv("revenue_attribution_results.csv", index=False)
    
    return df

def main():
    try:
        print("Starting revenue attribution analysis...")
        print(f"Using API Key (first 6 chars): {KLAVIYO_API_KEY[:6]}...")
        
        # Fetch campaigns and flows
        campaigns, flows = get_campaigns_and_flows()
        print(f"Found {len(campaigns)} campaigns and {len(flows)} flows")
        
        # Process revenue attribution
        df_revenue = process_revenue_attribution(campaigns, flows)
        
        print("\nAnalysis complete! Results have been saved to:")
        if not df_revenue.empty:
            print("- revenue_attribution_results.json")
            print("- revenue_attribution_results.csv")
            print("\nDataFrame Preview:")
            print(df_revenue.head())
            print("\nDataFrame Info:")
            print(df_revenue.info())
        else:
            print("No data was retrieved. Please check the API responses above for more details.")
        
    except Exception as e:
        print(f"An error occurred in the main process: {str(e)}")
        raise  # Re-raise the exception to see the full traceback

if __name__ == "__main__":
    main()