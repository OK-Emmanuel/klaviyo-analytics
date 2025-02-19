import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("KLAVIYO_API_KEY")


# Fetching API Key
api_key = os.getenv("KLAVIYO_API_KEY")

if not api_key:
    raise ValueError("No API key found")


base_url = "https://a.klaviyo.com/api"



def get_profiles():
    endpoint = "/profiles/"
    url = f"{base_url}{endpoint}"
    headers = {
        "Authorization": f"Klaviyo-API-Key {api_key}",
        "REVISION": "2025-01-15",
    }

    # Make a GET request to the Klaviyo API
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        return response.text # Return the error message
    
    # Testing a function

# if __name__ == "__main__":
#     profiles = get_profiles()
#     print(profiles)


        

