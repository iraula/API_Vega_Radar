import requests
import json
from datetime import datetime

def test_promo_api(codigo, activo=1):
    # API configuration
    BASE_URL = "http://127.0.0.1:3045"
    ENDPOINT = "/promos_nestle"
    url = f"{BASE_URL}{ENDPOINT}"
    
    # Request data
    data = {
        "codigo": codigo,
        "activo": activo
    }
    
    # Headers for the request
    headers = {
        "Content-Type": "application/json"
    }
    
    print(f"\n{'='*50}")
    print(f"Testing Promo API at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    print(f"\nRequest Details:")
    print(f"- URL: {url}")
    print(f"- Payload: {json.dumps(data, indent=2)}")
    
    try:
        # Make the POST request
        response = requests.post(url, json=data, headers=headers)
        
        print(f"\nResponse Details:")
        print(f"- Status Code: {response.status_code}")
        print(f"- Response Time: {response.elapsed.total_seconds():.3f} seconds")
        
        # Check if the response is JSON
        try:
            response_data = response.json()
            print(f"- Response Body: {json.dumps(response_data, indent=2)}")
        except json.JSONDecodeError:
            print(f"- Response Body: {response.text}")
        
        # Status code handling
        if response.status_code == 200:
            print("\nResult: SUCCESS ✅")
        elif response.status_code == 404:
            print("\nResult: NOT FOUND ❌")
            print("No promotions found for the given code.")
        elif response.status_code == 400:
            print("\nResult: BAD REQUEST ❌")
            print("Please check the request parameters.")
        else:
            print(f"\nResult: ERROR ❌")
            print(f"Unexpected status code: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("\nResult: CONNECTION ERROR ❌")
        print(f"Could not connect to {url}")
        print("Please make sure the API server is running.")
    except Exception as e:
        print("\nResult: UNEXPECTED ERROR ❌")
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    # Test cases
    CODIGO = "VAN-RUT-OTR-2025-02-847"
    ACTIVO = 0  # You can change this value to test different active states
    
    test_promo_api(CODIGO, ACTIVO)