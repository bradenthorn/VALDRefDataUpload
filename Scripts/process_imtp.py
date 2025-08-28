import pandas as pd
import numpy as np
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery
import uuid
from datetime import datetime
import asyncio
import aiohttp

# Import your existing helper functions
from token_generator import get_access_token
from VALDapiHelpers import get_profiles, FD_Tests_by_Profile, get_FD_results, FORCEDECKS_URL, TENANT_ID

# =================================================================================
# CONFIGURATION
# =================================================================================
PROJECT_ID = "vald-ref-data"
DATASET_ID = "athlete_performance_db"
TABLE_ID = "imtp_results"
CREDENTIALS_FILE = 'gcp_credentials.json'
CONCURRENT_REQUESTS = 10 # Number of API calls to make at the same time

# =================================================================================
# REVISED: Define the schema to include the new columns
# =================================================================================
IMTP_RESULTS_SCHEMA = [
    {'name': 'result_id', 'type': 'STRING'},
    {'name': 'assessment_id', 'type': 'STRING'},
    {'name': 'athlete_name', 'type': 'STRING'},
    {'name': 'test_date', 'type': 'DATE'},
    {'name': 'age_at_test', 'type': 'INT64'},
    {'name': 'ISO_BM_REL_FORCE_PEAK_Trial_N_kg', 'type': 'FLOAT64'},
    {'name': 'PEAK_VERTICAL_FORCE_Trial_N', 'type': 'FLOAT64'}
]

# =================================================================================
# Asynchronous function to fetch a single test result
# =================================================================================
async def fetch_single_test_result(session, test_id, token):
    """Asynchronously fetches results for a single test ID."""
    url = f"{FORCEDECKS_URL}/v2019q3/teams/{TENANT_ID}/tests/{test_id}/trials"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                # Note: This part calls your original synchronous function.
                # For maximum performance, this could be rewritten to be fully async.
                pivoted_df = get_FD_results(test_id, token)
                return test_id, pivoted_df
            else:
                print(f"    Error fetching test {test_id}: Status {response.status}")
                return test_id, None
    except Exception as e:
        print(f"    Exception fetching test {test_id}: {e}")
        return test_id, None

# =================================================================================
# Main processing logic to use asyncio
# =================================================================================
async def process_and_upload_all_best_imtp():
    """
    Asynchronously fetches all IMTP tests, finds the best trial for each,
    and uploads them to the imtp_results table in BigQuery.
    """
    # --- Step 1: Authentication and Setup (Synchronous) ---
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        print("Successfully loaded GCP credentials.")
    except Exception as e:
        print(f"ERROR: Could not load credentials. {e}")
        return

    print("Fetching access token...")
    token = get_access_token()

    print("Fetching all athlete profiles...")
    profiles = get_profiles(token)
    if profiles.empty:
        print("No profiles found. Exiting.")
        return

    # --- Step 2: Collect all IMTP test sessions from all athletes (Synchronous) ---
    all_imtp_test_sessions = []
    for index, athlete in profiles.iterrows():
        # We need the full athlete object to get DOB later
        tests_df = FD_Tests_by_Profile("2020-01-01T00:00:00Z", athlete['profileId'], token)
        if tests_df is not None and not tests_df.empty:
            imtp_tests = tests_df[tests_df['testType'] == 'IMTP']
            for _, test_session in imtp_tests.iterrows():
                # Store the full athlete and test info together
                all_imtp_test_sessions.append({'athlete': athlete, 'test': test_session})
    
    if not all_imtp_test_sessions:
        print("No IMTP tests found across all profiles.")
        return
        
    print(f"\nFound a total of {len(all_imtp_test_sessions)} IMTP tests to process.")

    # --- Step 3: Fetch all test results concurrently (Asynchronous) ---
    all_best_trials_for_upload = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_single_test_result(session, session_info['test']['testId'], token) for session_info in all_imtp_test_sessions]
        
        for i in range(0, len(tasks), CONCURRENT_REQUESTS):
            batch_tasks = tasks[i:i+CONCURRENT_REQUESTS]
            results = await asyncio.gather(*batch_tasks)
            
            # --- Step 4: Process the results from the completed batch ---
            for test_id, pivoted_trials_df in results:
                if pivoted_trials_df is None or pivoted_trials_df.empty:
                    continue

                # Find the original session info that corresponds to this result
                session_info = next((s for s in all_imtp_test_sessions if s['test']['testId'] == test_id), None)
                if not session_info:
                    continue

                athlete_info = session_info['athlete']
                test_info = session_info['test']

                pivoted_trials_df.set_index('metric_id', inplace=True)
                
                try:
                    peak_force_row = pivoted_trials_df.loc['PEAK_VERTICAL_FORCE_Trial_N']
                except KeyError:
                    continue

                trial_columns = [col for col in peak_force_row.index if 'trial' in col]
                peak_force_values = pd.to_numeric(peak_force_row[trial_columns], errors='coerce')
                
                if peak_force_values.isnull().all():
                    continue

                best_trial_col_name = peak_force_values.idxmax()
                best_trial_series = pivoted_trials_df[best_trial_col_name]
                
                # --- REVISED: Calculate age at test safely ---
                test_date = pd.to_datetime(test_info['modifiedDateUtc']).date()
                age_at_test = None  # Default to None (which will become NULL in BigQuery)

                # Check if the dateOfBirth from the API is valid before calculating age
                if pd.notna(athlete_info['dateOfBirth']):
                    dob = pd.to_datetime(athlete_info['dateOfBirth']).date()
                    age_at_test = test_date.year - dob.year - ((test_date.month, test_date.day) < (dob.month, dob.day))

                final_record = {
                    'result_id': str(uuid.uuid4()),
                    'assessment_id': test_id,
                    'athlete_name': athlete_info['fullName'],
                    'test_date': test_date,
                    'age_at_test': age_at_test,
                    'ISO_BM_REL_FORCE_PEAK_Trial_N_kg': pd.to_numeric(best_trial_series.get('ISO_BM_REL_FORCE_PEAK_Trial_N/kg'), errors='coerce'),
                    'PEAK_VERTICAL_FORCE_Trial_N': pd.to_numeric(best_trial_series.get('PEAK_VERTICAL_FORCE_Trial_N'), errors='coerce')
                }
                all_best_trials_for_upload.append(final_record)
                print(f"  Processed best trial for {athlete_info['fullName']} on {test_date}.")

    # --- Step 5: Upload all results at once (Synchronous) ---
    if not all_best_trials_for_upload:
        print("\nNo valid best trials found to upload.")
        return

    final_df = pd.DataFrame(all_best_trials_for_upload)

    print(f"\nUploading {len(final_df)} total best trials to BigQuery table '{TABLE_ID}'...")
    try:
        pandas_gbq.to_gbq(
            final_df,
            destination_table=f"{DATASET_ID}.{TABLE_ID}",
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists='append',
            table_schema=IMTP_RESULTS_SCHEMA
        )
        print("Upload successful!")
    except Exception as e:
        print(f"An error occurred during the BigQuery upload: {e}")

# =================================================================================
# MAIN EXECUTION
# =================================================================================
if __name__ == "__main__":
    asyncio.run(process_and_upload_all_best_imtp())
