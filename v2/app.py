import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
import time
import json
import pandas as pd
import streamlit as st

load_dotenv()

KLAVIYO_API_URL = "https://a.klaviyo.com/api"

# Shared API request function
def make_klaviyo_request(endpoint, api_key, params=None, method="GET", json_body=None):
    """Make a request to Klaviyo API with enhanced error handling"""
    headers = {
        "Authorization": f"Klaviyo-API-Key {api_key}",
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
            return make_klaviyo_request(endpoint, api_key, params, method, json_body)
        
        if response.status_code != 200:
            print(f"Error response for {endpoint}: {response.text}")
            return None
            
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"API Request failed for {endpoint}: {str(e)}")
        return None

# Feature 1: Revenue Attribution Split
def get_campaigns_and_flows(api_key):
    """Fetch campaigns and flows from the last 365 days"""
    campaign_list = []
    flow_list = []
    start_date = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"
    
    next_page = None
    while True:
        if next_page is None:
            params = {"filter": f"equals(messages.channel,'email'),greater-or-equal(updated_at,{start_date})"}
        else:
            params = {"page[cursor]": next_page, "filter": f"equals(messages.channel,'email'),greater-or-equal(updated_at,{start_date})"}
        
        campaigns = make_klaviyo_request("campaigns", api_key, params=params)
        if campaigns is None or 'data' not in campaigns:
            break
        campaign_list.extend(campaigns['data'])
        if 'links' in campaigns and 'next' in campaigns['links'] and campaigns['links']['next']:
            next_page = campaigns['links']['next'].split("?page[cursor]=")[1]
        else:
            break

    next_page = None
    while True:
        if next_page is None:
            params = {"filter": f"greater-or-equal(updated,{start_date})", "sort": "updated"}
        else:
            params = {"page[cursor]": next_page, "filter": f"greater-or-equal(updated,{start_date})"}
        
        flows = make_klaviyo_request("flows", api_key, params=params)
        if flows is None or 'data' not in flows:
            break
        flow_list.extend(flows['data'])
        if 'links' in flows and 'next' in flows['links'] and flows['links']['next']:
            next_page = flows['links']['next'].split("?page[cursor]=")[1]
        else:
            break
    
    return campaign_list, flow_list

def get_revenue_data(api_key, metric_id):
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
    response = make_klaviyo_request("metric-aggregates", api_key, method="POST", json_body=json_body)
    return response["data"]["attributes"]["data"] if response and "data" in response else []

def split_revenue(api_key, metric_id):
    """Fetch events and split revenue into new vs. recurring"""
    start_date = "2024-01-01T00:00:00Z"
    filter_str = f'greater-or-equal(datetime,{start_date})'
    params = {"filter": filter_str}
    print(f"Fetching events with filter: {filter_str}")
    
    events = []
    next_page = None
    
    while True:
        if next_page:
            params["page[cursor]"] = next_page
        response = make_klaviyo_request("events", api_key, params=params)
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
    
    revenue_split = {}
    for event in events:
        campaign_id = event["attributes"]["properties"].get("$attributed_message", 
                                                          event["attributes"]["properties"].get("$attributed_flow", ""))
        revenue = event["attributes"]["properties"].get("$value", 0.0)
        profile_id = event["relationships"]["profile"]["data"]["id"]
        timestamp = event["attributes"]["datetime"]
        
        prior_filter = f'equals(metric_id,"{metric_id}"),less-than(datetime,{timestamp})'
        prior_params = {"filter": prior_filter}
        prior_response = make_klaviyo_request(f"profiles/{profile_id}/events", api_key, params=prior_params)
        prior_count = len(prior_response["data"]) if prior_response and "data" in prior_response else 0
        
        if campaign_id not in revenue_split:
            revenue_split[campaign_id] = {"new": 0.0, "recurring": 0.0}
        if prior_count == 0:
            revenue_split[campaign_id]["new"] += revenue
        else:
            revenue_split[campaign_id]["recurring"] += revenue
    
    return revenue_split

def process_revenue_attribution(api_key, campaigns, flows, revenue_data, revenue_split):
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
            "klaviyo_api_key": api_key,
            "campaign_id": campaign_id,
            "campaign_name": campaign.get('attributes', {}).get('name', 'Unknown'),
            "send_time": campaign.get('attributes', {}).get('created_at', datetime.utcnow().isoformat())[:10],
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
            "klaviyo_api_key": api_key,
            "campaign_id": flow_id,
            "campaign_name": flow.get('attributes', {}).get('name', 'Unknown Flow'),
            "send_time": flow.get('attributes', {}).get('updated_at', datetime.utcnow().isoformat())[:10],
            "total_attributed_revenue": total,
            "new_customers_revenue": split["new"],
            "recurring_customers_revenue": split["recurring"]
        })
    
    df = pd.DataFrame(results)
    if not df.empty:
        df.to_json("revenue_attribution_results.json", orient="records", indent=2)
        df.to_csv("revenue_attribution_results.csv", index=False)
    return df

def revenue_attribution_analysis(api_key):
    """Run Feature 1 analysis"""
    try:
        print("Starting revenue attribution analysis...")
        campaigns, flows = get_campaigns_and_flows(api_key)
        print(f"Found {len(campaigns)} campaigns and {len(flows)} flows")
        
        metrics = make_klaviyo_request("metrics", api_key)
        metric_id = next((m["id"] for m in metrics["data"] if m["attributes"]["name"] == "Placed Order"), None)
        if not metric_id:
            print("No Placed Order metric found")
            return None
        
        revenue_data = get_revenue_data(api_key, metric_id)
        revenue_split = split_revenue(api_key, metric_id)
        df = process_revenue_attribution(api_key, campaigns, flows, revenue_data, revenue_split)
        
        print("\nRevenue Attribution Analysis complete!")
        return df
    except Exception as e:
        print(f"An error occurred in revenue attribution: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None

# Feature 2: Product Purchase Attribution
def get_product_purchases(api_key, metric_id):
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
        response = make_klaviyo_request("events", api_key, params=params)
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
    
    product_data = {}
    seen_orders = set()
    
    for event in events:
        order_id = event["attributes"]["properties"].get("OrderId", "")
        if order_id in seen_orders:
            continue
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

def process_product_attribution(api_key, campaigns, flows, product_data):
    """Process product purchase attribution"""
    results = []
    campaign_dict = {c["id"]: c for c in campaigns}
    flow_dict = {f["id"]: f for f in flows}
    
    for product_id, data in product_data.items():
        for campaign_id in data["campaign_ids"]:
            source = campaign_dict.get(campaign_id, flow_dict.get(campaign_id, {}))
            results.append({
                "klaviyo_api_key": api_key,
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

def product_attribution_analysis(api_key):
    """Run Feature 2 analysis"""
    try:
        print("Starting product purchase attribution analysis...")
        campaigns, flows = get_campaigns_and_flows(api_key)
        print(f"Found {len(campaigns)} campaigns and {len(flows)} flows")
        
        metrics = make_klaviyo_request("metrics", api_key)
        metric_id = next((m["id"] for m in metrics["data"] if m["attributes"]["name"] == "Placed Order"), None)
        if not metric_id:
            print("No Placed Order metric found")
            return None
        
        product_data = get_product_purchases(api_key, metric_id)
        df = process_product_attribution(api_key, campaigns, flows, product_data)
        
        print("\nProduct Attribution Analysis complete!")
        return df
    except Exception as e:
        print(f"An error occurred in product attribution: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None

# Feature 3: Klaviyo Attribution Share
def get_revenue_share(api_key, metric_id):
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
        response = make_klaviyo_request("events", api_key, params=params)
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
    
    daily_data = {}
    seen_orders = set()
    
    for event in events:
        order_id = event["attributes"]["properties"].get("OrderId", "")
        if order_id in seen_orders:
            continue
        seen_orders.add(order_id)
        
        date = event["attributes"]["datetime"][:10]
        revenue = float(event["attributes"]["properties"].get("$value", 0.0))
        is_attributed = bool(event["attributes"]["properties"].get("$attributed_message") or 
                            event["attributes"]["properties"].get("$attributed_flow"))
        
        if date not in daily_data:
            daily_data[date] = {"total": 0.0, "attributed": 0.0}
        daily_data[date]["total"] += revenue
        if is_attributed:
            daily_data[date]["attributed"] += revenue
    
    results = []
    for date, data in daily_data.items():
        total = data["total"]
        attributed = data["attributed"]
        share = (attributed / total * 100) if total > 0 else 0.0
        results.append({
            "klaviyo_api_key": api_key,
            "date": date,
            "total_shop_revenue": total,
            "klaviyo_attributed_revenue": attributed,
            "klaviyo_revenue_share": share
        })
    
    return results

def process_revenue_share(api_key, results):
    """Process and save revenue share data"""
    df = pd.DataFrame(results)
    if not df.empty:
        df.to_json("revenue_share_results.json", orient="records", indent=2)
        df.to_csv("revenue_share_results.csv", index=False)
    return df

def revenue_share_analysis(api_key):
    """Run Feature 3 analysis"""
    try:
        print("Starting revenue share analysis...")
        
        metrics = make_klaviyo_request("metrics", api_key)
        metric_id = next((m["id"] for m in metrics["data"] if m["attributes"]["name"] == "Placed Order"), None)
        if not metric_id:
            print("No Placed Order metric found")
            return None
        
        share_data = get_revenue_share(api_key, metric_id)
        df = process_revenue_share(api_key, share_data)
        
        print("\nRevenue Share Analysis complete!")
        return df
    except Exception as e:
        print(f"An error occurred in revenue share: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None

# Streamlit Interface
def main():
    st.title("Klaviyo Marketing Analytics Dashboard")
    
    with st.sidebar:
        st.header("API Configuration")
        private_api_key = st.text_input("Private API Key (Klaviyo API Key)", type="password")
        if private_api_key:
            st.success("API Key loaded!")
        analyze_button = st.button("Run All Analyses")

    if analyze_button:
        if not private_api_key:
            st.error("Please provide a Private API Key")
        else:
            print(f"Loaded API Key: {private_api_key[:6]}...")
            tab1, tab2, tab3 = st.tabs(["Revenue Attribution", "Product Attribution", "Revenue Share"])
            
            # Feature 1: Revenue Attribution
            with tab1:
                st.header("Revenue Attribution Split")
                with st.spinner("Running revenue attribution analysis..."):
                    df_revenue = revenue_attribution_analysis(private_api_key)
                    if df_revenue is not None and not df_revenue.empty:
                        st.success("Analysis completed!")
                        st.dataframe(df_revenue)
                        csv = df_revenue.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name="revenue_attribution_results.csv",
                            mime="text/csv"
                        )
                        json_data = df_revenue.to_json(orient="records", indent=2)
                        st.download_button(
                            label="Download JSON",
                            data=json_data,
                            file_name="revenue_attribution_results.json",
                            mime="application/json"
                        )
                    else:
                        st.warning("No data retrieved or analysis failed")

            # Feature 2: Product Attribution
            with tab2:
                st.header("Product Purchase Attribution")
                with st.spinner("Running product attribution analysis..."):
                    df_products = product_attribution_analysis(private_api_key)
                    if df_products is not None and not df_products.empty:
                        st.success("Analysis completed!")
                        st.dataframe(df_products)
                        csv = df_products.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name="product_attribution_results.csv",
                            mime="text/csv"
                        )
                        json_data = df_products.to_json(orient="records", indent=2)
                        st.download_button(
                            label="Download JSON",
                            data=json_data,
                            file_name="product_attribution_results.json",
                            mime="application/json"
                        )
                    else:
                        st.warning("No data retrieved or analysis failed")

            # Feature 3: Revenue Share
            with tab3:
                st.header("Klaviyo Revenue Share")
                with st.spinner("Running revenue share analysis..."):
                    df_share = revenue_share_analysis(private_api_key)
                    if df_share is not None and not df_share.empty:
                        st.success("Analysis completed!")
                        st.dataframe(df_share)
                        csv = df_share.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name="revenue_share_results.csv",
                            mime="text/csv"
                        )
                        json_data = df_share.to_json(orient="records", indent=2)
                        st.download_button(
                            label="Download JSON",
                            data=json_data,
                            file_name="revenue_share_results.json",
                            mime="application/json"
                        )
                    else:
                        st.warning("No data retrieved or analysis failed")

if __name__ == "__main__":
    main()