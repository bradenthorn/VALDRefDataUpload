import pandas as pd
import numpy as np
import uuid
from datetime import datetime
import asyncio
import aiohttp
import json
import sys
import os
import logging

# Import your existing helper functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from VALDapiHelpers import get_access_token, get_profiles, FD_Tests_by_Profile
from config import settings
from bigquery_helpers import upload_to_bigquery, bq_client

logger = logging.getLogger(__name__)

# =================================================================================
# CONFIGURATION
# =================================================================================
PROJECT_ID = settings.gcp.project_id
DATASET_ID = settings.gcp.dataset_id
TABLE_ID = settings.gcp.ppu_table_id
FORCEDECKS_URL = settings.vald_api.forcedecks_url
TENANT_ID = settings.vald_api.tenant_id
CONCURRENT_REQUESTS = 10
DELAY_BETWEEN_BATCHES = 2

UNIT_MAP = {
    "Newton": "N",
    "Millisecond": "ms",
    "Second": "s",
    "Percent": "%",
    "Kilo": "kg",
    "Pound": "lb",
    "No Unit": "",
    "Newton Per Second": "N/s",
    "Newton Per Kilo": "N/kg",
    "Newton Per Second Per Kilo": "N/s/kg",
    "Centimeter": "cm",
    "Inch": "in"
}

# Helper to ensure consistent metric_id formatting
def sanitize_metric_id(metric_id):
    """Return a metric_id with `/` and `.` replaced by underscores."""
    if not isinstance(metric_id, str):
        return metric_id
    return metric_id.replace('/', '_').replace('.', '_')

# Mapping from metric_id to BigQuery column names
METRIC_ID_TO_BQ_COL = {
    sanitize_metric_id('ECCENTRIC_BRAKING_RFD_Trial_N/s'): 'ECCENTRIC_BRAKING_RFD_Trial_N_s',
    sanitize_metric_id('MEAN_ECCENTRIC_FORCE_Asym_Trial_N'): 'MEAN_ECCENTRIC_FORCE_Asym_N',
    sanitize_metric_id('MEAN_TAKEOFF_FORCE_Asym_Trial_N'): 'MEAN_TAKEOFF_FORCE_Asym_N',
    sanitize_metric_id('PEAK_CONCENTRIC_FORCE_Asym_Trial_N'): 'PEAK_CONCENTRIC_FORCE_Asym_N',
    sanitize_metric_id('PEAK_CONCENTRIC_FORCE_Trial_N'): 'PEAK_CONCENTRIC_FORCE_Trial_N',
    sanitize_metric_id('PEAK_ECCENTRIC_FORCE_Asym_Trial_N'): 'PEAK_ECCENTRIC_FORCE_Asym_N',
    sanitize_metric_id('RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N/kg'): 'RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N_kg',
    sanitize_metric_id('CONCENTRIC_DURATION_Trial_ms'): 'CONCENTRIC_DURATION_Trial_ms',
}


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
            unit_raw = res["definition"].get("unit", "")
            unit = UNIT_MAP.get(unit_raw, unit_raw)
            limb = res.get("limb")
            result_key = res["definition"].get("result", "")
            # Fix: Avoid duplicate 'Trial' in metric_id
            if limb == "Trial":
                metric_id = f"{result_key}_Trial_{unit}"
            else:
                metric_id = f"{result_key}_{limb}_Trial_{unit}"
            metric_id = sanitize_metric_id(metric_id)
            flat_result = {
                "value": res.get("value"),
                "limb": limb,
                "result_key": result_key,
                "unit": unit,
                "metric_id": metric_id
            }
            all_results.append(flat_result)
    if not all_results:
        return None
    df = pd.DataFrame(all_results)
    # Use the precomputed metric_id
    # df['metric_id'] = (df['result_key'].astype(str) + '_' + df['limb'].astype(str) + '_Trial_' + df['unit'].astype(str))
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
# Main processing logic for Push-Up Tests
# =================================================================================
async def main_pipeline():
    """Main asynchronous pipeline to fetch, process, and upload all PPU tests."""
    if bq_client is None:
        print("BigQuery client not available. Cannot upload.")
        return

    token = get_access_token()
    print("Fetching all athlete profiles...")
    profiles = get_profiles(token)
    if profiles.empty:
        print("No profiles found. Exiting.")
        return

    print("--- RUNNING IN TEST MODE: PROCESSING ALL ATHLETES ---")
    # profiles = profiles.head(50)  # Remove this line to process all athletes

    all_ppu_test_sessions = []
    print("Collecting all PPU test sessions for the selected athletes...")
    # Ensure profiles is a pandas DataFrame before iterating
    if not isinstance(profiles, pd.DataFrame):
        profiles = pd.DataFrame(profiles)
    for index, athlete in enumerate(profiles.itertuples(index=False)):
        if isinstance(index, int) and index > 0 and index % 50 == 0:
             token = get_access_token()
        tests_df = FD_Tests_by_Profile("2020-01-01T00:00:00Z", getattr(athlete, 'profileId', None), token)
        if tests_df is not None and not tests_df.empty:
            ppu_tests = tests_df[tests_df['testType'] == 'PPU']
            if not isinstance(ppu_tests, pd.DataFrame):
                ppu_tests = pd.DataFrame(ppu_tests)
            for _, test_session in ppu_tests.iterrows():
                all_ppu_test_sessions.append({'athlete': athlete, 'test': test_session})
    
    if not all_ppu_test_sessions:
        print("No PPU tests found for the selected athletes.")
        return
        
    print(f"\nFound a total of {len(all_ppu_test_sessions)} PPU tests to process.")

    all_best_trials_for_upload = []
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(all_ppu_test_sessions), CONCURRENT_REQUESTS):
            batch_token = get_access_token()
            batch_sessions = all_ppu_test_sessions[i:i+CONCURRENT_REQUESTS]
            tasks = [fetch_and_process_single_test(session, session_info['test']['testId'], batch_token) for session_info in batch_sessions]
            results = await asyncio.gather(*tasks)
            
            for test_id, pivoted_trials_df in results:
                if pivoted_trials_df is None or pivoted_trials_df.empty:
                    continue

                session_info = next((s for s in all_ppu_test_sessions if s['test']['testId'] == test_id), None)
                if not session_info:
                    continue

                athlete_info = session_info['athlete']
                test_info = session_info['test']

                pivoted_trials_df.set_index('metric_id', inplace=True)
                
                peak_force_metric = next((m for m in pivoted_trials_df.index if 'PEAK_CONCENTRIC_FORCE' in m and 'kg' not in m and 'Asym' not in m), None)
                if not peak_force_metric:
                    print(f"  Skipping test {test_id}: Could not find the absolute Peak Concentric Force metric.")
                    continue

                peak_force_row = pivoted_trials_df.loc[peak_force_metric]
                trial_columns = [col for col in peak_force_row.index if 'trial' in col]
                peak_force_values = peak_force_row[trial_columns]
                # Ensure peak_force_values is a pandas Series before calling dropna()
                if not isinstance(peak_force_values, pd.Series):
                    try:
                        peak_force_values = pd.Series(peak_force_values)
                    except Exception:
                        continue
                peak_force_values = pd.to_numeric(peak_force_values, errors='coerce')
                if isinstance(peak_force_values, pd.Series):
                    peak_force_values = peak_force_values.dropna()
                else:
                    continue
                
                if peak_force_values.empty:
                    continue

                best_trial_col_name = peak_force_values.idxmax()
                best_trial_series = pivoted_trials_df[best_trial_col_name]
                
                best_trial_series.index = best_trial_series.index.map(sanitize_metric_id)
                logger.debug(f"DEBUG: best_trial_series.index after replacements: {list(best_trial_series.index)}")

                test_date = pd.to_datetime(test_info['modifiedDateUtc']).date()
                age_at_test = None
                # Robustly handle date_of_birth for both string and Timestamp types
                date_of_birth = getattr(athlete_info, 'dateOfBirth', None)
                if date_of_birth is not None and str(date_of_birth).strip() and str(date_of_birth).lower() != 'nan':
                    try:
                        dob = pd.to_datetime(date_of_birth).date()
                        if 1920 < dob.year < datetime.now().year:
                            age_at_test = test_date.year - dob.year - ((test_date.month, test_date.day) < (dob.month, dob.day))
                    except Exception as e:
                        print(f"Could not parse date_of_birth '{date_of_birth}' for athlete {getattr(athlete_info, 'fullName', None)}: {e}")

                def get_metric_value(exact_metric_id):
                    metric = sanitize_metric_id(exact_metric_id)
                    value = best_trial_series.get(metric)
                    print(f"Looking for metric: {metric}, value: {value}")
                    logger.debug(f"Looking for metric: {exact_metric_id}, value: {value}")
                    return pd.to_numeric(value, errors='coerce') if value is not None else None

                # Build the final record with mapped BigQuery column names
                final_record = {
                    'result_id': str(uuid.uuid4()),
                    'assessment_id': test_id,
                    'athlete_name': getattr(athlete_info, 'fullName', None),
                    'test_date': test_date,
                    'age_at_test': age_at_test,
                    'CONCENTRIC_DURATION_Trial_ms': get_metric_value('CONCENTRIC_DURATION_Trial_ms'),
                }
                for metric_id, bq_col in METRIC_ID_TO_BQ_COL.items():
                    final_record[bq_col] = get_metric_value(metric_id)
                all_best_trials_for_upload.append(final_record)
                print(f"  Successfully processed PPU for {getattr(athlete_info, 'fullName', None)} on {test_date}.")

            print(f"\n--- Batch {i//CONCURRENT_REQUESTS + 1} complete. Pausing for {DELAY_BETWEEN_BATCHES} seconds... ---\n")
            await asyncio.sleep(DELAY_BETWEEN_BATCHES)

    if not all_best_trials_for_upload:
        print("\nNo valid PPU results found to upload.")
        return

    final_df = pd.DataFrame(all_best_trials_for_upload)
    # Restrict DataFrame to only the required columns
    BQ_COLS = [
        'result_id', 'assessment_id', 'athlete_name', 'test_date', 'age_at_test',
        'CONCENTRIC_DURATION_Trial_ms',
        'ECCENTRIC_BRAKING_RFD_Trial_N_s',
        'MEAN_ECCENTRIC_FORCE_Asym_N',
        'MEAN_TAKEOFF_FORCE_Asym_N',
        'PEAK_CONCENTRIC_FORCE_Asym_N',
        'PEAK_CONCENTRIC_FORCE_Trial_N',
        'PEAK_ECCENTRIC_FORCE_Asym_N',
        'RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N_kg',
    ]
    final_df = final_df[[col for col in BQ_COLS if col in final_df.columns]]

    # Call the new, more robust upload function
    upload_to_bigquery(final_df, TABLE_ID)

# =================================================================================
# MAIN EXECUTION
# =================================================================================
if __name__ == "__main__":
    asyncio.run(main_pipeline())
