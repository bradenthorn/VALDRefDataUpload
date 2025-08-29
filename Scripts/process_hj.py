import pandas as pd
import uuid
from datetime import datetime
import asyncio
import aiohttp
from logging_utils import get_logger

# Import your existing helper functions
from VALDapiHelpers import get_profiles, FD_Tests_by_Profile, get_access_token, process_json_to_pivoted_df
from config import settings
from bigquery_helpers import upload_to_bigquery, bq_client

logger = get_logger(__name__)

# =================================================================================
# CONFIGURATION
# =================================================================================
PROJECT_ID = settings.gcp.project_id
DATASET_ID = settings.gcp.dataset_id
TABLE_ID = settings.gcp.hj_table_id
FORCEDECKS_URL = settings.vald_api.forcedecks_url
TENANT_ID = settings.vald_api.tenant_id
CONCURRENT_REQUESTS = 10
DELAY_BETWEEN_BATCHES = 2

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
                logger.error("Error fetching test %s: Status %s", test_id, response.status)
                return test_id, None
    except Exception as e:
        logger.error("Exception fetching test %s: %s", test_id, e)
        return test_id, None

# =================================================================================
# Main processing logic for Hop Jumps
# =================================================================================
async def main_pipeline():
    """Main asynchronous pipeline to fetch, process, and upload all HJ tests."""
    if bq_client is None:
        logger.error("BigQuery client not available. Exiting.")
        return

    token = get_access_token()
    logger.info("Fetching all athlete profiles...")
    profiles = get_profiles(token)
    if profiles.empty:
        logger.info("No profiles found. Exiting.")
        return

    # =================================================================================
    # CHANGE: Limit the DataFrame to the first 10 profiles for quick testing
    # =================================================================================


    # --- Step 2: Collect all HJ test sessions ---
    all_hj_test_sessions = []
    logger.info("Collecting all HJ test sessions for the selected athletes...")
    for index, athlete in profiles.iterrows():
        if index > 0 and index % 50 == 0:
             token = get_access_token()
        tests_df = FD_Tests_by_Profile("2020-01-01T00:00:00Z", athlete['profileId'], token)
        if tests_df is not None and not tests_df.empty:
            hj_tests = tests_df[tests_df['testType'] == 'HJ']
            for _, test_session in hj_tests.iterrows():
                all_hj_test_sessions.append({'athlete': athlete, 'test': test_session})
    
    if not all_hj_test_sessions:
        logger.info("No Hop Jump tests found for the selected athletes.")
        return

    logger.info("Found a total of %d HJ tests to process.", len(all_hj_test_sessions))

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
                        logger.warning("Skipping test %s: Missing Flight Time or Contact Time.", test_id)
                        continue

                    # Extract the trial values as numeric series
                    trial_columns = [col for col in flight_time_row.columns if 'trial' in col]
                    flight_times = pd.to_numeric(flight_time_row.iloc[0][trial_columns], errors='coerce')
                    contact_times = pd.to_numeric(contact_time_row.iloc[0][trial_columns], errors='coerce')

                    # Calculate RSI for each trial: Flight Time (in seconds) / Contact Time (in seconds)
                    # The data is in milliseconds, so we divide both by 1000, which cancels out.
                    rsi_per_trial = (flight_times / contact_times).dropna()
                    
                except (KeyError, IndexError):
                    logger.warning("Skipping test %s: Could not find required metrics for RSI calculation.", test_id)
                    continue

                if rsi_per_trial.empty:
                    logger.warning("Skipping test %s: No valid trials to calculate RSI.", test_id)
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
                logger.info(
                    "Processed HJ for %s on %s. Avg RSI: %.2f",
                    athlete_info['fullName'],
                    test_date,
                    avg_of_best_5_rsi,
                )

            logger.info(
                "Batch %d of %d complete. Pausing for %d seconds...",
                i // CONCURRENT_REQUESTS + 1,
                len(all_hj_test_sessions) // CONCURRENT_REQUESTS + 1,
                DELAY_BETWEEN_BATCHES,
            )
            await asyncio.sleep(DELAY_BETWEEN_BATCHES)

    # --- Step 5: Upload all results at once ---
    if not all_best_rsi_averages:
        logger.info("No valid HJ results found to upload after processing all batches.")
        return

    final_df = pd.DataFrame(all_best_rsi_averages)

    logger.info(
        "Uploading %d total best HJ results to BigQuery table '%s'...",
        len(final_df),
        TABLE_ID,
    )
    upload_to_bigquery(final_df, TABLE_ID)

# =================================================================================
# MAIN EXECUTION
# =================================================================================
if __name__ == "__main__":
    asyncio.run(main_pipeline())
