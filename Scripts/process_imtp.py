import pandas as pd
import uuid
from datetime import datetime
import asyncio
import aiohttp
import logging

# Import your existing helper functions
from config import settings
from VALDapiHelpers import (
    get_profiles,
    FD_Tests_by_Profile,
    process_json_to_pivoted_df,
    get_access_token,
)
from bigquery_helpers import upload_to_bigquery, bq_client

logger = logging.getLogger(__name__)

# =================================================================================
# CONFIGURATION
# =================================================================================
PROJECT_ID = settings.gcp.project_id
DATASET_ID = settings.gcp.dataset_id
TABLE_ID = settings.gcp.imtp_table_id
FORCEDECKS_URL = settings.vald_api.forcedecks_url
TENANT_ID = settings.vald_api.tenant_id
CONCURRENT_REQUESTS = 10 # Number of API calls to make at the same time

METRIC_ISO_BM_REL_FORCE_PEAK = 'ISO_BM_REL_FORCE_PEAK_Trial_N_kg'
METRIC_PEAK_VERTICAL_FORCE = 'PEAK_VERTICAL_FORCE_Trial_N'
REQUIRED_METRIC_IDS = [
    METRIC_ISO_BM_REL_FORCE_PEAK,
    METRIC_PEAK_VERTICAL_FORCE,
]  

# =================================================================================
# Asynchronous function to fetch a single test result
# =================================================================================
async def fetch_single_test_result(session, test_id, token):
    """Asynchronously fetches results for a single test ID.

    If the request returns a 401 (Unauthorized), a new access token is
    fetched and the request is retried once.
    """
    url = f"{FORCEDECKS_URL}/v2019q3/teams/{TENANT_ID}/tests/{test_id}/trials"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                test_data = await response.json()
                pivoted_df = process_json_to_pivoted_df(test_data)
                return test_id, pivoted_df
            if response.status == 401:
                # Token might be expired; refresh and retry once
                refreshed_token = get_access_token()
                retry_headers = {"Authorization": f"Bearer {refreshed_token}"}
                async with session.get(url, headers=retry_headers) as retry_response:
                    if retry_response.status == 200:
                        test_data = await retry_response.json()
                        pivoted_df = process_json_to_pivoted_df(test_data)
                        return test_id, pivoted_df
                    print(
                        f"    Error fetching test {test_id}: Status {retry_response.status} after token refresh"
                    )
                    return test_id, None
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
    if bq_client is None:
        print("BigQuery client not available. Cannot upload.")
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
        for i in range(0, len(all_imtp_test_sessions), CONCURRENT_REQUESTS):
            # Refresh the access token before each batch to avoid expiration
            token = get_access_token()
            batch_sessions = all_imtp_test_sessions[i:i+CONCURRENT_REQUESTS]
            tasks = [
                fetch_single_test_result(session, session_info['test']['testId'], token)
                for session_info in batch_sessions
            ]
            results = await asyncio.gather(*tasks)
            
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

                missing_metrics = [m for m in REQUIRED_METRIC_IDS if m not in best_trial_series.index]
                if missing_metrics:
                    logger.warning(f"Test {test_id} missing metrics: {', '.join(missing_metrics)}")
                
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
    upload_to_bigquery(final_df, TABLE_ID)

# =================================================================================
# MAIN EXECUTION
# =================================================================================
if __name__ == "__main__":
    asyncio.run(process_and_upload_all_best_imtp())
