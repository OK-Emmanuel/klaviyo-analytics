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

print(f"Loaded API Key: {KLAVIYO_API_KEY[:6]}...")

def make_klaviyo_request(endpoint, params=None, method="GET", json_body=None):
    """Make a request to Klaviyo API with enhanced error handling"""
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
        "Accept": "application/json",
        "revision": "2025-01-15"
    }
    url = f"{KLAVIYO_API_URL}/{endpoint.lstrip('/')}"
    
    try:
        if method == "POST":
            response = requests.post(url, headers=headers, params=params, json=json_body)
        else:
            response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limit reached. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            return make_klaviyo_request(endpoint, params, method, json_body)
        
        if response.status_code != 200:
            print(f"Error response for {endpoint}: {response.text}")
            return None
            
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"API Request failed for {endpoint}: {str(e)}")
        return None

def get_revenue_share(metric_id):
    """Fetch Placed Order events and calculate daily revenue share"""
    start_date = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"
    filter_str = f'greater-or-equal(datetime,{start_date})'
    params = {"filter": filter_str}
    print(f"Fetching events with filter: {filter_str}")
    
    events = []
    next_page = None
    
    while True:
        if next_page:
            params["page[cursor]"] = next_page
        response = make_klaviyo_request("events", params=params)
        if response is None or "data" not in response:
            break
        filtered_events = [e for e in response["data"] if e["relationships"]["metric"]["data"]["id"] == metric_id]
        events.extend(filtered_events)
        print(f"Fetched {len(filtered_events)} Placed Order events this page")
        
        if "links" in response and "next" in response["links"] and response["links"]["next"] is not None:
            next_link = response["links"]["next"]
            if "?page[cursor]=" in next_link:
                next_page = next_link.split("?page[cursor]=")[1]
            elif "page%5Bcursor%5D=" in next_link:
                next_page = next_link.split("page%5Bcursor%5D=")[1]
            else:
                print(f"Unexpected next link format: {next_link}")
                break
        else:
            break
    
    # Aggregate daily data
    daily_data = {}
    seen_orders = set()  # For deduplication
    
    for event in events:
        order_id = event["attributes"]["properties"].get("OrderId", "")
        if order_id in seen_orders:
            continue
        seen_orders.add(order_id)
        
        date = event["attributes"]["datetime"][:10]  # YYYY-MM-DD
        revenue = float(event["attributes"]["properties"].get("$value", 0.0))
        is_attributed = bool(event["attributes"]["properties"].get("$attributed_message") or 
                            event["attributes"]["properties"].get("$attributed_flow"))
        
        if date not in daily_data:
            daily_data[date] = {"total": 0.0, "attributed": 0.0}
        daily_data[date]["total"] += revenue
        if is_attributed:
            daily_data[date]["attributed"] += revenue
    
    # Calculate share
    results = []
    for date, data in daily_data.items():
        total = data["total"]
        attributed = data["attributed"]
        share = (attributed / total * 100) if total > 0 else 0.0
        results.append({
            "klaviyo_api_key": KLAVIYO_API_KEY,
            "date": date,
            "total_shop_revenue": total,
            "klaviyo_attributed_revenue": attributed,
            "klaviyo_revenue_share": share
        })
    
    return results

def process_revenue_share(results):
    """Process and save revenue share data"""
    df = pd.DataFrame(results)
    if not df.empty:
        df.to_json("revenue_share_results.json", orient="records", indent=2)
        df.to_csv("revenue_share_results.csv", index=False)
    return df

def main():
    try:
        print("Starting revenue share analysis...")
        
        # Fetch metric ID
        metrics = make_klaviyo_request("metrics")
        metric_id = next((m["id"] for m in metrics["data"] if m["attributes"]["name"] == "Placed Order"), None)
        if not metric_id:
            print("No Placed Order metric found")
            return
        
        # Fetch and process revenue share
        share_data = get_revenue_share(metric_id)
        df = process_revenue_share(share_data)
        
        print("\nAnalysis complete! Results saved to:")
        if not df.empty:
            print("- revenue_share_results.json")
            print("- revenue_share_results.csv")
            print("\nDataFrame Preview:")
            print(df.head())
        else:
            print("No data retrieved - DataFrame is empty")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()