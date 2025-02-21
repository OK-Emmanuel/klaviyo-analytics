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
PUBLIC_API_KEY =  os.getenv("PUBLIC_API_KEY") 
if not KLAVIYO_API_KEY:
    raise ValueError("No API key found. Please create a .env file with your KLAVIYO_API_KEY")

KLAVIYO_API_URL = "https://a.klaviyo.com/api"
KLAVIYO_TRACK_URL = "https://a.klaviyo.com/api/track"

print(f"Loaded API Key: {KLAVIYO_API_KEY[:6]}...")

def make_klaviyo_request(endpoint, params=None, method="GET", json_body=None, use_track=False):
    """Make a request to Klaviyo API with enhanced error handling"""
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
        "Accept": "application/json",
        "revision": "2025-01-15"
    }
    url = KLAVIYO_TRACK_URL if use_track else f"{KLAVIYO_API_URL}/{endpoint.lstrip('/')}"
    
    try:
        if method == "POST":
            response = requests.post(url, headers=headers, params=params, json=json_body)
        else:
            response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limit reached. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            return make_klaviyo_request(endpoint, params, method, json_body, use_track)
        
        if response.status_code != 200:
            print(f"Error response for {endpoint}: {response.text}")
            return None
            
        return response.json() if not use_track else response.text
    
    except requests.exceptions.RequestException as e:
        print(f"API Request failed for {endpoint}: {str(e)}")
        return None

def get_campaigns_and_flows():
    """Fetch both campaigns and flows with 365-day filter"""
    campaign_list = []
    flow_list = []
    start_date = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"
    
    # Fetch campaigns with pagination
    next_page = None
    while True:
        if next_page is None:
            params = {"filter": f"equals(messages.channel,'email'),greater-or-equal(updated_at,{start_date})"}
        else:
            params = {"page[cursor]": next_page, "filter": f"equals(messages.channel,'email'),greater-or-equal(updated_at,{start_date})"}
        
        campaigns = make_klaviyo_request("campaigns", params=params)
        if campaigns is None:
            break
        if 'data' in campaigns:
            campaign_list.extend(campaigns['data'])
            if 'links' in campaigns and 'next' in campaigns['links'] and campaigns['links']['next']:
                next_page = campaigns['links']['next'].split("?page[cursor]=")[1]
            else:
                break
        else:
            break

    # Fetch flows with pagination
    next_page = None
    while True:
        if next_page is None:
            params = {"filter": f"greater-or-equal(updated,{start_date})", "sort": "updated"}
        else:
            params = {"page[cursor]": next_page, "filter": f"greater-or-equal(updated,{start_date})"}
        
        flows = make_klaviyo_request("flows", params=params)
        if flows is None:
            break
        if 'data' in flows:
            flow_list.extend(flows['data'])
            if 'links' in flows and 'next' in flows['links'] and flows['links']['next']:
                next_page = flows['links']['next'].split("?page[cursor]=")[1]
            else:
                break
        else:
            break
    
    return campaign_list, flow_list

def get_revenue_data(metric_id):
    """Fetch revenue data for campaigns and flows"""
    start_date = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"
    end_date = datetime.utcnow().isoformat() + "Z"
    json_body = {
        "data": {
            "type": "metric-aggregate",
            "attributes": {
                "measurements": ["sum_value"],
                "filter": [f"greater-or-equal(datetime,{start_date})", f"less-than(datetime,{end_date})"],
                "by": ["$attributed_message", "$attributed_flow"],
                "metric_id": metric_id
            }
        }
    }
    response = make_klaviyo_request("metric-aggregates", method="POST", json_body=json_body)
    return response["data"]["attributes"]["data"] if response and "data" in response else []



def split_revenue(metric_id):
    """Fetch events and split revenue into new vs. recurring"""
    start_date = "2024-01-01T00:00:00Z"  # Wider net to catch all
    filter_str = f'greater-or-equal(datetime,{start_date})'
    params = {"filter": filter_str}
    print(f"Fetching events with filter: {filter_str}")
    
    events = []
    next_page = None
    
    while True:
        if next_page:
            params["page[cursor]"] = next_page
        response = make_klaviyo_request("events", params=params)
        if response is None:
            print(f"Failed to fetch events with filter: {filter_str}")
            break
        if "data" not in response:
            break
        print(f"Total events in response: {len(response['data'])}")  # Debug raw count
        # Filter by metric ID in relationships
        filtered_events = [e for e in response["data"] if e["relationships"]["metric"]["data"]["id"] == metric_id]
        events.extend(filtered_events)
        print(f"Fetched {len(filtered_events)} Placed Order events this page")
        if filtered_events and len(filtered_events) > 0:
            print(f"Sample event: {json.dumps(filtered_events[0], indent=2)}")
        
        # Fixed pagination handling
        if "links" in response and "next" in response["links"] and response["links"]["next"] is not None:
            # Handle both URL formats - with or without query params
            if "?page[cursor]=" in response["links"]["next"]:
                next_page = response["links"]["next"].split("?page[cursor]=")[1]
            elif "page%5Bcursor%5D=" in response["links"]["next"]:
                next_page = response["links"]["next"].split("page%5Bcursor%5D=")[1]
            else:
                # If format is different, extract cursor another way or break
                print(f"Unexpected next link format: {response['links']['next']}")
                break
        else:
            break
    
    revenue_split = {}
    for event in events:
        campaign_id = event["attributes"]["properties"].get("$attributed_message", event["attributes"]["properties"].get("$attributed_flow", ""))
        revenue = event["attributes"]["properties"].get("$value", 0.0)
        profile_id = event["relationships"]["profile"]["data"]["id"]
        timestamp = event["attributes"]["datetime"]
        
        # Check prior orders
        prior_filter = f'equals(metric_id,"{metric_id}"),less-than(datetime,{timestamp})'
        prior_params = {"filter": prior_filter}
        print(f"Checking prior events for profile {profile_id} with filter: {prior_filter}")
        prior_response = make_klaviyo_request(f"profiles/{profile_id}/events", params=prior_params)
        prior_count = len(prior_response["data"]) if prior_response and "data" in prior_response else 0
        
        if campaign_id not in revenue_split:
            revenue_split[campaign_id] = {"new": 0.0, "recurring": 0.0}
        if prior_count == 0:
            revenue_split[campaign_id]["new"] += revenue
        else:
            revenue_split[campaign_id]["recurring"] += revenue
    
    return revenue_split

def process_revenue_attribution(campaigns, flows, revenue_data, revenue_split):
    """Process revenue attribution with new vs. recurring split"""
    results = []
    revenue_dict = {item["dimensions"][0]: item["measurements"]["sum_value"][0] for item in revenue_data if item["dimensions"][0]}
    
    for campaign in campaigns:
        campaign_id = campaign.get('id', '')
        if not campaign_id:
            continue
        total = revenue_dict.get(campaign_id, 0.0)
        split = revenue_split.get(campaign_id, {"new": 0.0, "recurring": 0.0})
        results.append({
            "campaign_id": campaign_id,
            "campaign_name": campaign.get('attributes', {}).get('name', 'Unknown'),
            "send_time": campaign.get('attributes', {}).get('created_at', datetime.utcnow().isoformat()),
            "total_attributed_revenue": total,
            "new_customers_revenue": split["new"],
            "recurring_customers_revenue": split["recurring"]
        })
    
    for flow in flows:
        flow_id = flow.get('id', '')
        if not flow_id:
            continue
        total = revenue_dict.get(flow_id, 0.0)
        split = revenue_split.get(flow_id, {"new": 0.0, "recurring": 0.0})
        results.append({
            "campaign_id": flow_id,
            "campaign_name": flow.get('attributes', {}).get('name', 'Unknown Flow'),
            "send_time": flow.get('attributes', {}).get('updated_at', datetime.utcnow().isoformat()),
            "total_attributed_revenue": total,
            "new_customers_revenue": split["new"],
            "recurring_customers_revenue": split["recurring"]
        })
    
    df = pd.DataFrame(results)
    if not df.empty:
        df.to_json("revenue_attribution_results.json", orient="records", indent=2)
        df.to_csv("revenue_attribution_results.csv", index=False)
    return df


def main_analysis_only():
    try:
        print("Starting revenue attribution analysis (skipping simulation)...")
        
        # Fetch campaigns and flows
        campaigns, flows = get_campaigns_and_flows()
        print(f"Found {len(campaigns)} campaigns and {len(flows)} flows")
        
        # Fetch metric ID
        metrics = make_klaviyo_request("metrics")
        metric_id = next((m["id"] for m in metrics["data"] if m["attributes"]["name"] == "Placed Order"), None)
        if not metric_id:
            print("No Placed Order metric found")
            return
        
        print(f"Using Placed Order metric ID: {metric_id}")
        
        # Fetch revenue data
        revenue_data = get_revenue_data(metric_id)
        print(f"Raw revenue data: {json.dumps(revenue_data, indent=2)}")
        
        revenue_split = split_revenue(metric_id)
        print(f"Revenue split data: {json.dumps(revenue_split, indent=2)}")
        
        # Process and output
        df_revenue = process_revenue_attribution(campaigns, flows, revenue_data, revenue_split)
        
        print("\nAnalysis complete! Results saved to:")
        if not df_revenue.empty:
            print("- revenue_attribution_results.json")
            print("- revenue_attribution_results.csv")
            print("\nDataFrame Preview:")
            print(df_revenue.head())
        else:
            print("No data retrieved - DataFrame is empty")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main_analysis_only()
