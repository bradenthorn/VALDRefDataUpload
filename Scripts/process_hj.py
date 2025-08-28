import pandas as pd
import numpy as np
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery
import uuid
from datetime import datetime
import asyncio
import aiohttp
import json

# Import your existing helper functions
from token_generator import get_access_token
from VALDapiHelpers import get_profiles, FD_Tests_by_Profile, FORCEDECKS_URL, TENANT_ID

# =================================================================================
# CONFIGURATION
# =================================================================================
PROJECT_ID = "vald-ref-data"
DATASET_ID = "athlete_performance_db"
TABLE_ID = "hj_results"
CREDENTIALS_FILE = 'gcp_credentials.json'
CONCURRENT_REQUESTS = 10
DELAY_BETWEEN_BATCHES = 2

# =================================================================================
# Define the schema for the hj_results table
# =================================================================================
HJ_RESULTS_SCHEMA = [
    {'name': 'result_id', 'type': 'STRING'},
    {'name': 'assessment_id', 'type': 'STRING'},
    {'name': 'athlete_name', 'type': 'STRING'},
    {'name': 'test_date', 'type': 'DATE'},
    {'name': 'age_at_test', 'type': 'INT64'},
    {'name': 'hop_rsi_avg_best_5', 'type': 'FLOAT64'}
]

# =================================================================================
# HELPER FUNCTION to process the raw JSON from the API
# =================================================================================
def process_json_to_pivoted_df(test_data_json):
    """Takes the raw JSON from a test result and pivots it into a DataFrame."""
    if not test_data_json or not isinstance(test_data_json, list):
        return None

    all_results = []
    for trial in test_data_json:
        results = trial.get("results", [])
        for res in results:
            flat_result = {
                "value": res.get("value"),
                "limb": res.get("limb"),
                "result_key": res["definition"].get("result", ""),
                "unit": res["definition"].get("unit", "")
            }
            all_results.append(flat_result)

    if not all_results:
        return None

    df = pd.DataFrame(all_results)
    df['metric_id'] = (df['result_key'].astype(str) + '_' + df['limb'].astype(str) + '_Trial_' + df['unit'].astype(str))
    df['trial'] = df.groupby('metric_id').cumcount() + 1
    pivot = df.pivot_table(index='metric_id', columns='trial', values='value', aggfunc='first')
    pivot.columns = [f'trial {c}' for c in pivot.columns]
    return pivot.reset_index()

# =================================================================================
# Asynchronous function to fetch and process a single test result
# =================================================================================
async def fetch_and_process_single_test(session, test_id, token):
    """Asynchronously fetches results for a single test ID and processes the JSON."""
    url = f"{FORCEDECKS_URL}/v2019q3/teams/{TENANT_ID}/tests/{test_id}/trials"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        async with session.get(url, headers=headers, timeout=30) as response:
            if response.status == 200:
                json_data = await response.json()
                pivoted_df = process_json_to_pivoted_df(json_data)
                return test_id, pivoted_df
            else:
                print(f"    Error fetching test {test_id}: Status {response.status}")
                return test_id, None
    except Exception as e:
        print(f"    Exception fetching test {test_id}: {e}")
        return test_id, None

# =================================================================================
# Main processing logic for Hop Jumps
# =================================================================================
async def main_pipeline():
    """Main asynchronous pipeline to fetch, process, and upload all HJ tests."""
    # --- Step 1: Authentication and Profile Fetching ---
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        print("Successfully loaded GCP credentials.")
    except Exception as e:
        print(f"ERROR: Could not load credentials. {e}")
        return

    token = get_access_token()
    print("Fetching all athlete profiles...")
    profiles = get_profiles(token)
    if profiles.empty:
        print("No profiles found. Exiting.")
        return

    # =================================================================================
    # CHANGE: Limit the DataFrame to the first 10 profiles for quick testing
    # =================================================================================


    # --- Step 2: Collect all HJ test sessions ---
    all_hj_test_sessions = []
    print("Collecting all HJ test sessions for the selected athletes...")
    for index, athlete in profiles.iterrows():
        if index > 0 and index % 50 == 0:
             token = get_access_token()
        tests_df = FD_Tests_by_Profile("2020-01-01T00:00:00Z", athlete['profileId'], token)
        if tests_df is not None and not tests_df.empty:
            hj_tests = tests_df[tests_df['testType'] == 'HJ']
            for _, test_session in hj_tests.iterrows():
                all_hj_test_sessions.append({'athlete': athlete, 'test': test_session})
    
    if not all_hj_test_sessions:
        print("No Hop Jump tests found for the selected athletes.")
        return
        
    print(f"\nFound a total of {len(all_hj_test_sessions)} HJ tests to process.")

    # --- Step 3: Fetch all test results concurrently ---
    all_best_rsi_averages = []
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(all_hj_test_sessions), CONCURRENT_REQUESTS):
            batch_token = get_access_token()
            
            batch_sessions = all_hj_test_sessions[i:i+CONCURRENT_REQUESTS]
            tasks = [fetch_and_process_single_test(session, session_info['test']['testId'], batch_token) for session_info in batch_sessions]
            
            results = await asyncio.gather(*tasks)
            
            # --- Step 4: Process the results from the completed batch ---
            for test_id, pivoted_trials_df in results:
                if pivoted_trials_df is None or pivoted_trials_df.empty:
                    continue

                session_info = next((s for s in all_hj_test_sessions if s['test']['testId'] == test_id), None)
                if not session_info:
                    continue

                athlete_info = session_info['athlete']
                test_info = session_info['test']

                pivoted_trials_df.set_index('metric_id', inplace=True)
                
                # =================================================================
                # FINAL FIX: Manually calculate RSI from its raw components
                # =================================================================
                try:
                    # Find the rows for flight time and contact time
                    flight_time_row = pivoted_trials_df.loc[pivoted_trials_df.index.str.contains('HOP_FLIGHT_TIME')]
                    contact_time_row = pivoted_trials_df.loc[pivoted_trials_df.index.str.contains('HOP_CONTACT_TIME')]

                    if flight_time_row.empty or contact_time_row.empty:
                        print(f"  Skipping test {test_id}: Missing Flight Time or Contact Time.")
                        continue

                    # Extract the trial values as numeric series
                    trial_columns = [col for col in flight_time_row.columns if 'trial' in col]
                    flight_times = pd.to_numeric(flight_time_row.iloc[0][trial_columns], errors='coerce')
                    contact_times = pd.to_numeric(contact_time_row.iloc[0][trial_columns], errors='coerce')

                    # Calculate RSI for each trial: Flight Time (in seconds) / Contact Time (in seconds)
                    # The data is in milliseconds, so we divide both by 1000, which cancels out.
                    rsi_per_trial = (flight_times / contact_times).dropna()
                    
                except (KeyError, IndexError):
                    print(f"  Skipping test {test_id}: Could not find required metrics for RSI calculation.")
                    continue
                
                if rsi_per_trial.empty:
                    print(f"  Skipping test {test_id}: No valid trials to calculate RSI.")
                    continue

                # Now, find the average of the 5 best *correctly calculated* RSI values
                avg_of_best_5_rsi = rsi_per_trial.nlargest(5).mean()
                
                test_date = pd.to_datetime(test_info['modifiedDateUtc']).date()
                age_at_test = None
                if pd.notna(athlete_info['dateOfBirth']):
                    dob = pd.to_datetime(athlete_info['dateOfBirth']).date()
                    if 1920 < dob.year < datetime.now().year:
                        age_at_test = test_date.year - dob.year - ((test_date.month, test_date.day) < (dob.month, dob.day))

                final_record = {
                    'result_id': str(uuid.uuid4()), 'assessment_id': test_id,
                    'athlete_name': athlete_info['fullName'], 'test_date': test_date, 'age_at_test': age_at_test,
                    'hop_rsi_avg_best_5': avg_of_best_5_rsi
                }
                all_best_rsi_averages.append(final_record)
                print(f"  Successfully processed HJ for {athlete_info['fullName']} on {test_date}. Avg RSI: {avg_of_best_5_rsi:.2f}")

            print(f"\n--- Batch {i//CONCURRENT_REQUESTS + 1} of {len(all_hj_test_sessions)//CONCURRENT_REQUESTS + 1} complete. Pausing for {DELAY_BETWEEN_BATCHES} seconds... ---\n")
            await asyncio.sleep(DELAY_BETWEEN_BATCHES)

    # --- Step 5: Upload all results at once ---
    if not all_best_rsi_averages:
        print("\nNo valid HJ results found to upload after processing all batches.")
        return

    final_df = pd.DataFrame(all_best_rsi_averages)

    print(f"\nUploading {len(final_df)} total best HJ results to BigQuery table '{TABLE_ID}'...")
    try:
        pandas_gbq.to_gbq(
            final_df,
            destination_table=f"{DATASET_ID}.{TABLE_ID}",
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists='append',
            table_schema=HJ_RESULTS_SCHEMA
        )
        print("Upload successful!")
    except Exception as e:
        print(f"An error occurred during the BigQuery upload: {e}")

# =================================================================================
# MAIN EXECUTION
# =================================================================================
if __name__ == "__main__":
    asyncio.run(main_pipeline())
