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

def get_campaigns_and_flows():
    """Fetch campaigns and flows from the last 365 days"""
    campaign_list = []
    flow_list = []
    start_date = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"
    
    # Fetch campaigns
    next_page = None
    while True:
        if next_page is None:
            params = {"filter": f"equals(messages.channel,'email'),greater-or-equal(updated_at,{start_date})"}
        else:
            params = {"page[cursor]": next_page, "filter": f"equals(messages.channel,'email'),greater-or-equal(updated_at,{start_date})"}
        
        campaigns = make_klaviyo_request("campaigns", params=params)
        if campaigns is None or 'data' not in campaigns:
            break
        campaign_list.extend(campaigns['data'])
        if 'links' in campaigns and 'next' in campaigns['links'] and campaigns['links']['next']:
            next_page = campaigns['links']['next'].split("?page[cursor]=")[1]
        else:
            break

    # Fetch flows
    next_page = None
    while True:
        if next_page is None:
            params = {"filter": f"greater-or-equal(updated,{start_date})", "sort": "updated"}
        else:
            params = {"page[cursor]": next_page, "filter": f"greater-or-equal(updated,{start_date})"}
        
        flows = make_klaviyo_request("flows", params=params)
        if flows is None or 'data' not in flows:
            break
        flow_list.extend(flows['data'])
        if 'links' in flows and 'next' in flows['links'] and flows['links']['next']:
            next_page = flows['links']['next'].split("?page[cursor]=")[1]
        else:
            break
    
    return campaign_list, flow_list

def get_product_purchases(metric_id):
    """Fetch product purchase data from Placed Order events"""
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
        
        # Improved pagination handling
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
    
    # Process product data
    product_data = {}
    seen_orders = set()  # For deduplication
    
    for event in events:
        order_id = event["attributes"]["properties"].get("OrderId", "")
        if order_id in seen_orders:
            continue  # Skip duplicates
        seen_orders.add(order_id)
        
        campaign_id = event["attributes"]["properties"].get("$attributed_message", 
                                                          event["attributes"]["properties"].get("$attributed_flow", ""))
        if not campaign_id:
            continue
        
        items = event["attributes"]["properties"].get("Items", [])
        for item in items:
            product_id = item.get("ProductID", "unknown")
            if product_id not in product_data:
                product_data[product_id] = {
                    "campaign_ids": set(),
                    "product_name": item.get("ProductName", "Unknown"),
                    "units_sold": 0,
                    "product_type": item.get("Categories", ["Unknown"])[0],
                    "revenue": 0.0
                }
            product_data[product_id]["campaign_ids"].add(campaign_id)
            product_data[product_id]["units_sold"] += int(item.get("Quantity", 0))
            product_data[product_id]["revenue"] += float(item.get("ItemPrice", 0.0)) * int(item.get("Quantity", 0))
    
    return product_data

def process_product_attribution(campaigns, flows, product_data):
    """Process product purchase attribution"""
    results = []
    campaign_dict = {c["id"]: c for c in campaigns}
    flow_dict = {f["id"]: f for f in flows}
    
    for product_id, data in product_data.items():
        for campaign_id in data["campaign_ids"]:
            source = campaign_dict.get(campaign_id, flow_dict.get(campaign_id, {}))
            results.append({
                "klaviyo_api_key": KLAVIYO_API_KEY,
                "campaign_id": campaign_id,
                "campaign_name": source.get("attributes", {}).get("name", "Unknown"),
                "send_time": source.get("attributes", {}).get("created_at", 
                                                            source.get("attributes", {}).get("updated_at", 
                                                                                           datetime.utcnow().isoformat()))[:10],
                "products": [{
                    "product_id": product_id,
                    "product_name": data["product_name"],
                    "units_sold": data["units_sold"],
                    "product_type": data["product_type"],
                    "revenue": data["revenue"]
                }]
            })
    
    df = pd.DataFrame(results)
    if not df.empty:
        df.to_json("product_attribution_results.json", orient="records", indent=2)
        df.to_csv("product_attribution_results.csv", index=False)
    return df

def main():
    try:
        print("Starting product purchase attribution analysis...")
        
        # Fetch campaigns and flows
        campaigns, flows = get_campaigns_and_flows()
        print(f"Found {len(campaigns)} campaigns and {len(flows)} flows")
        
        # Fetch metric ID
        metrics = make_klaviyo_request("metrics")
        metric_id = next((m["id"] for m in metrics["data"] if m["attributes"]["name"] == "Placed Order"), None)
        if not metric_id:
            print("No Placed Order metric found")
            return
        
        # Fetch and process product data
        product_data = get_product_purchases(metric_id)
        df = process_product_attribution(campaigns, flows, product_data)
        
        print("\nAnalysis complete! Results saved to:")
        if not df.empty:
            print("- product_attribution_results.json")
            print("- product_attribution_results.csv")
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