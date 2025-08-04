import requests
import time
import pandas as pd
import os
from dotenv import load_dotenv
import sys
from pathlib import Path

load_dotenv()

# Replace with your actual Consumer Key and Consumer Secret from USPS Developer Portal
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# USPS API Endpoints
USPS_TOKEN_URL = "https://apis.usps.com/oauth2/v3/token"

# *** IMPORTANT: Based on https://developer.usps.com/addressesv3#tag/Resources/operation/get-address ***
# This endpoint uses GET and expects parameters as query strings.
USPS_ADDRESS_API_URL = "https://apis.usps.com/addresses/v3/address"

# Batch processing
BATCH_SIZE = 2
DELAY = 1 # 1 second delay between batches

# --- Function to Get Auth Token ---
# Retrieves an OAuth2 access token from the USPS API.
def get_usps_token(client_id: str, client_secret: str, token_url: str) -> str:
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "addresses"
    }

    print("\n--- Attempting to get Auth Token ---")

    try:
        response = requests.post(token_url, headers=headers, data=payload)
        response.raise_for_status()

        token_data = response.json()
        access_token = token_data.get('access_token')

        if not access_token:
            raise ValueError(f"Access token not found in response: {token_data}")

        print(f"Successfully retrieved token. Expires in: {token_data.get('expires_in')} seconds.")
        return access_token

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error during token retrieval: {e}")
        if e.response is not None:
            print(f"Response Status Code: {e.response.status_code}")
            print(f"Response Body: {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Network or request error during token retrieval: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during token retrieval: {e}")
        raise

# --- Function to Validate Address (using GET method with query parameters) ---
# Validates a single address using the USPS Addresses v3 API (GET method) and returns structured results.
def validate_usps_address(address_details: dict, access_token: str, api_url: str) -> dict:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    # Parameters for the GET request go into 'params'
    params = {
        "streetAddress": address_details["STREET"], # Use original CSV header name
        "city": address_details["CITY"],             # Use original CSV header name
        "state": address_details["STATE"],           # Use original CSV header name
        "ZIPCode": str(address_details["POST_CODE"]) # Use original CSV header name, ensure string
    }

    # print(f"  Validating: {address_details['STREET']}, {address_details['CITY']}, {address_details['STATE']} {address_details['POST_CODE']}")
    # print(f"  Request Parameters: {params}") # Uncomment for debugging

    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()

        response_data = response.json()
        
        # print(f"  Raw USPS API Response:\n{json.dumps(response_data, indent=2)}") # Uncomment for debugging full response

        validated_address = response_data.get("address", {})

        # Extract fields based on confirmed USPS response structure
        street = validated_address.get("streetAddress")
        city = validated_address.get("city")
        state = validated_address.get("state")
        zip_code = validated_address.get("ZIPCode")
        zip_plus_4 = validated_address.get("ZIPPlus4")

        # Return a dictionary with the desired keys
        return {
            "KEY": address_details["KEY"], # Original KEY from CSV
            "original_STREET": address_details["STREET"], # Original STREET from CSV
            "original_CITY": address_details["CITY"],     # Original CITY from CSV
            "original_STATE": address_details["STATE"],   # Original STATE from CSV
            "original_POST_CODE": address_details["POST_CODE"], # Original POST_CODE from CSV
            "validated_STREET": street,
            "validated_CITY": city,
            "validated_STATE": state,
            "validated_ZIPCode": zip_code, # Use the exact key from USPS response
            "validated_ZIPPlus4": zip_plus_4, # Use the exact key from USPS response
            "full_zip4": f"{zip_code}-{zip_plus_4}" if zip_code and zip_plus_4 else (zip_code if zip_code else "N/A"),
            "is_valid": bool(street and city and state and zip_code) # Simple check for validation success
        }

    except requests.exceptions.HTTPError as e:
        print(f"  HTTP Error validating address for KEY {address_details.get('KEY', 'N/A')}: {e}")
        if e.response is not None:
            print(f"  Response Status Code: {e.response.status_code}")
            print(f"  Response Body: {e.response.text}")
        # Return partial result or error indicator for failed rows
        return {
            "KEY": address_details["KEY"],
            "original_STREET": address_details["STREET"],
            "original_CITY": address_details["CITY"],
            "original_STATE": address_details["STATE"],
            "original_POST_CODE": address_details["POST_CODE"],
            "validated_STREET": None,
            "validated_CITY": None,
            "validated_STATE": None,
            "validated_ZIPCode": None,
            "validated_ZIPPlus4": None,
            "full_zip4": "ERROR",
            "is_valid": False,
            "error_message": str(e) # Store the error message
        }
    except requests.exceptions.RequestException as e:
        print(f"  Network or request error validating address for KEY {address_details.get('KEY', 'N/A')}: {e}")
        return {
            "KEY": address_details["KEY"],
            "original_STREET": address_details["STREET"],
            "original_CITY": address_details["CITY"],
            "original_STATE": address_details["STATE"],
            "original_POST_CODE": address_details["POST_CODE"],
            "validated_STREET": None,
            "validated_CITY": None,
            "validated_STATE": None,
            "validated_ZIPCode": None,
            "validated_ZIPPlus4": None,
            "full_zip4": "ERROR",
            "is_valid": False,
            "error_message": str(e)
        }
    except Exception as e:
        print(f"  An unexpected error occurred for KEY {address_details.get('KEY', 'N/A')}: {e}")
        return {
            "KEY": address_details["KEY"],
            "original_STREET": address_details["STREET"],
            "original_CITY": address_details["CITY"],
            "original_STATE": address_details["STATE"],
            "original_POST_CODE": address_details["POST_CODE"],
            "validated_STREET": None,
            "validated_CITY": None,
            "validated_STATE": None,
            "validated_ZIPCode": None,
            "validated_ZIPPlus4": None,
            "full_zip4": "ERROR",
            "is_valid": False,
            "error_message": str(e)
        }

# --- Function to Load Input CSV ---
def load_input_csv() -> pd.DataFrame:
    # Ask the user for the input CSV filename
    filename = input("What is the filename? (include .csv): ").strip()

    # Validate extension rules (case-insensitive)
    lower_name = filename.lower()

    # Missing extension
    if '.' not in Path(filename).name:
        print("Missing .csv extension")
        sys.exit(1)

    # Excel extensions not allowed
    excel_exts = {'.xlsx', '.xlsm', '.xls', '.xltx', '.xltm'}
    if any(lower_name.endswith(ext) for ext in excel_exts):
        print("Please convert your Excel file to a .csv")
        sys.exit(1)

    # Must end with .csv
    if not lower_name.endswith('.csv'):
        print("Missing .csv extension")
        sys.exit(1)

    # Resolve data/ folder relative to this script
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / "data"
    csv_path = data_dir / filename

    # Check existence in data/
    if not csv_path.exists():
        print("filename not found in data/ folder. Please check this file has been moved to the right folder")
        sys.exit(1)

    # Load CSV
    try:
        df_local = pd.read_csv(csv_path)
        print(f"\nSuccessfully loaded {len(df_local)} records from '{csv_path}'.")
        return df_local
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        sys.exit(1)

# --- Main Workflow ---
def main():
    # 1. Get the Auth Token
    try:
        access_token = get_usps_token(CLIENT_ID, CLIENT_SECRET, USPS_TOKEN_URL)
    except Exception:
        print("Exiting due to token retrieval failure.")
        exit(1)

    # Ask for and load CSV from data/ folder
    try:
        df = load_input_csv()
        print(f"\nSuccessfully loaded {len(df)} records.")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        exit(1)

    # Prepare a list to store results for DataFrame creation
    all_validated_results = []

    # 3. Process addresses in batches
    num_rows = len(df)
    for i in range(0, num_rows, BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE]
        print(f"\nProcessing batch {i // BATCH_SIZE + 1} (rows {i} to {min(i + BATCH_SIZE, num_rows) - 1})...")

        for _, row in batch.iterrows():
            # Convert pandas Series row to a dict for the validate_usps_address function
            address_data = row.to_dict()
            validated_data = validate_usps_address(address_data, access_token, USPS_ADDRESS_API_URL)
            all_validated_results.append(validated_data)
        
        # Apply delay between batches, but not after the last batch
        if i + BATCH_SIZE < num_rows:
            print(f"Pausing for {DELAY} second(s) before next batch...")
            time.sleep(DELAY)

    # 4. Create and save the DataFrame
    if all_validated_results:
        validated_df = pd.DataFrame(all_validated_results)
        
        if validated_df["is_valid"].sum() == 0:
            print("\nNo valid addresses found. Please check the input data or API credentials.")
            sys.exit(1)
            
        output_csv_file = "data/validated_addresses.csv"
        validated_df.to_csv(output_csv_file, index=False)
        print(f"\nValidation complete. Results saved to '{output_csv_file}'.")
        print("\n--- Sample of Validated Data (first 5 rows) ---")
        print(validated_df.head())
    else:
        print("\nNo addresses were processed or validated.")


if __name__ == "__main__":
    main()