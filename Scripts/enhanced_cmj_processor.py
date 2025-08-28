"""
Enhanced CMJ Processor with Composite Scoring Integration
This script integrates composite scoring into the existing GCP pipeline for CMJ data.
"""

import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from Scripts.RandomScrips.CompositeScore import calculate_composite_score_per_trial, get_best_trial, CMJ_weights
from VALDapiHelpers import get_access_token, get_profiles, FD_Tests_by_Profile, get_FD_results
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Configuration
CREDENTIALS_FILE = 'gcp_credentials.json'
PROJECT_ID = 'vald-ref-data'  # Replace with your actual project ID
DATASET_ID = 'athlete_performance_db'
TABLE_ID = 'cmj_results'

def upload_to_bigquery(df, table_name, table_schema=None):
    """Upload DataFrame to BigQuery with proper error handling."""
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        pandas_gbq.to_gbq(
            df,
            destination_table=f"{DATASET_ID}.{table_name}",
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists='append',
            table_schema=table_schema
        )
        print(f"Successfully uploaded {len(df)} rows to {table_name}")
        return True
    except Exception as e:
        print(f"Error uploading to BigQuery: {e}")
        return False

def process_cmj_test_with_composite(test_id, token, assessment_id):
    """
    Process a single CMJ test and calculate composite scores.
    
    Args:
        test_id: VALD test ID
        token: Access token
        assessment_id: Assessment ID for GCP
    
    Returns:
        DataFrame ready for GCP upload with composite scores
    """
    
    # Fetch raw CMJ data
    print(f"Fetching CMJ data for test {test_id}...")
    raw_data = get_FD_results(test_id, token)

    if raw_data is None or raw_data.empty:
        print(f"No data found for test {test_id}")
        return None, None
    
    # Filter for CMJ-specific metrics
    cmj_metrics = [
        'BODY_WEIGHT_LBS_Trial_lb',
        'CONCENTRIC_DURATION_Trial_ms',
        'CONCENTRIC_IMPULSE_Trial_Ns',
        'CONCENTRIC_RFD_Trial_N_s',
        'ECCENTRIC_BRAKING_RFD_Trial_N_s',
        'JUMP_HEIGHT_IMP_MOM_Trial_cm',
        'PEAK_CONCENTRIC_FORCE_Trial_N',
        'PEAK_TAKEOFF_POWER_Trial_W',
        'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
        'CONCENTRIC_IMPULSE_P1_Trial_Ns',
        'CONCENTRIC_IMPULSE_P2_Trial_Ns',
        'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns',
        'RSI_MODIFIED_IMP_MOM_Trial_RSI_mod',
        'RSI_MODIFIED_Trial_RSI_mod',
        'CON_P2_CON_P1_IMPULSE_RATIO_Trial'
    ]
    
    # Filter data for CMJ metrics
    cmj_data = raw_data[raw_data['metric_id'].isin(cmj_metrics)]
    
    if cmj_data.empty:
        print(f"No CMJ metrics found for test {test_id}")
        return None, None
    
    # Pivot data to get trials as columns
    trial_cols = [col for col in cmj_data.columns if 'trial' in col.lower()]
    if not trial_cols:
        print(f"No trial data found for test {test_id}")
        return None, None
    
    # Create pivot table with metrics as index and trials as columns
    pivot_data = cmj_data.set_index('metric_id')[trial_cols]
    if not isinstance(pivot_data, pd.DataFrame):
        pivot_data = pd.DataFrame(pivot_data)
    # DEBUG: Print available metric names
    print(f"[DEBUG] Available metrics in pivot_data for test {test_id}: {list(pivot_data.index)}")
    # Calculate composite scores per trial
    best_trial_col, best_score, composite_scores, best_metrics = get_best_trial(pivot_data)
    if best_trial_col is None:
        print(f"No valid composite scores for test {test_id}")
        return None, None
    # DEBUG: Print best_metrics dict
    print(f"[DEBUG] best_metrics for test {test_id}: {best_metrics}")
    # Prepare DataFrame for upload using best_metrics dict
    # Only upload metrics in CMJ_weights
    required_metrics = list(CMJ_weights.keys())
    # Map API metric names to BigQuery-safe column names if needed
    metric_map = {
        'CONCENTRIC_IMPULSE_Trial_N/s': 'CONCENTRIC_IMPULSE_Trial_Ns',
        'ECCENTRIC_BRAKING_RFD_Trial_N/s': 'ECCENTRIC_BRAKING_RFD_Trial_N_s',
        'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
    }
    upload_dict = {}
    for metric in required_metrics:
        bq_col = metric_map.get(metric, metric)
        upload_dict[bq_col] = best_metrics.get(metric, float('nan'))
    gcp_data = pd.DataFrame([upload_dict])
    gcp_data['result_id'] = str(uuid.uuid4())
    gcp_data['assessment_id'] = assessment_id
    gcp_data['cmj_composite_score'] = best_score
    gcp_data.reset_index(drop=True, inplace=True)
    # Use the required schema
    gcp_schema = [
        {'name': 'result_id', 'type': 'STRING'},
        {'name': 'assessment_id', 'type': 'STRING'},
    ] + [
        {'name': metric_map.get(metric, metric), 'type': 'FLOAT64'} for metric in required_metrics
    ] + [
        {'name': 'cmj_composite_score', 'type': 'FLOAT64'},
    ]
    return gcp_data, gcp_schema

def process_all_cmj_tests_for_athlete(profile_id: str, token: str, assessment_id: str, athlete_name: str) -> list[pd.DataFrame]:
    """
    Process all CMJ tests for a given athlete and upload to GCP.
    
    Args:
        profile_id: VALD profile ID
        token: Access token
        assessment_id: Assessment ID for GCP
        athlete_name: Athlete's full name
    
    Returns:
        List of processed test results
    """
    
    # Fetch all tests for the athlete
    start_date = "2021-1-1 00:00:00"
    tests_df = FD_Tests_by_Profile(start_date, profile_id, token)
    
    if tests_df is None or tests_df.empty:
        print(f"No tests found for profile {profile_id}")
        return []
    
    # Filter for CMJ tests
    cmj_tests = tests_df[tests_df['testType']== 'CMJ']
    
    if cmj_tests.empty:
        print(f"No CMJ tests found for profile {profile_id}")
        return []
    
    print(f"Found {len(cmj_tests)} CMJ tests for profile {profile_id}")
    
    processed_results = []
    
    for _, test_row in cmj_tests.iterrows():
        test_id = test_row['testId']
        test_date = pd.to_datetime(test_row['modifiedDateUtc']).date()

        print(f"Processing CMJ test {test_id} from {test_date}...")

        # Process the test
        gcp_data, gcp_schema = process_cmj_test_with_composite(test_id, token, assessment_id)

        # Only append if gcp_data is a valid, non-empty DataFrame
        if isinstance(gcp_data, pd.DataFrame) and not gcp_data.empty:
            gcp_data['athlete_name'] = athlete_name
            processed_results.append(gcp_data)
            print(f"Successfully processed test {test_id}")
        else:
            print(f"Failed to process test {test_id}")
    
    return processed_results

def main_pipeline():
    """
    Main pipeline to process CMJ data with composite scoring for all athletes.
    """
    
    # Authentication
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        print("Successfully loaded GCP credentials.")
    except Exception as e:
        print(f"ERROR: Could not load credentials. {e}")
        return
    
    # Get access token
    token = get_access_token()
    if not token:
        print("Failed to get access token")
        return
    
    # Fetch all athlete profiles
    print("Fetching athlete profiles...")
    profiles = get_profiles(token)
    if profiles.empty:
        print("No profiles found. Exiting.")
        return
    
    print(f"Found {len(profiles)} athlete profiles")
    
    # Only process the first 10 athletes
    profiles = profiles.head(10)
    
    # Process each athlete
    all_results = []
    
    for index, athlete in profiles.iterrows():
        profile_id = str(athlete['profileId'])
        athlete_name = str(athlete['fullName'])
        
        print(f"\nProcessing athlete {index + 1}/{len(profiles)}: {athlete_name}")
        
        # Create assessment ID for this athlete
        assessment_id = str(uuid.uuid4())
        
        # Process all CMJ tests for this athlete
        athlete_results = process_all_cmj_tests_for_athlete(profile_id, token, assessment_id, athlete_name)
        
        if athlete_results:
            all_results.extend(athlete_results)
            print(f"Processed {len(athlete_results)} CMJ tests for {athlete_name}")
        else:
            print(f"No CMJ tests processed for {athlete_name}")
        
        # Add delay to avoid rate limiting
        if index < len(profiles) - 1:
            print("Waiting 2 seconds before next athlete...")
            import time
            time.sleep(2)
    
    # After collecting all_results and before upload
    if all_results:
        print(f"\nUploading {len(all_results)} total CMJ results to BigQuery...")
        combined_df = pd.concat(all_results, ignore_index=True)
        # Normalize composite scores to 50-100 scale
        min_score = combined_df['cmj_composite_score'].min()
        max_score = combined_df['cmj_composite_score'].max()
        if max_score != min_score:
            combined_df['cmj_composite_score'] = 50 + (combined_df['cmj_composite_score'] - min_score) / (max_score - min_score) * 50
        else:
            combined_df['cmj_composite_score'] = 100
        # Rename columns to BigQuery-safe names
        rename_map = {
            'ECCENTRIC_BRAKING_RFD_Trial_N/s': 'ECCENTRIC_BRAKING_RFD_Trial_N_s',
            'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
            'CONCENTRIC_DURATION_Trial/ms': 'CONCENTRIC_DURATION_Trial_ms',
        }
        combined_df.rename(columns=rename_map, inplace=True)
        # Print debug info before upload
        print("[DEBUG] Columns in combined_df before upload:", combined_df.columns.tolist())
        print("[DEBUG] First 5 rows of combined_df:")
        print(combined_df.head())
        # Upload all columns (including metrics and composite score)
        upload_to_bigquery(combined_df, TABLE_ID)
        
        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"Average Composite Score: {combined_df['cmj_composite_score'].mean():.3f}")
        print(f"Best Composite Score: {combined_df['cmj_composite_score'].max():.3f}")
        # print(f"Number of athletes with data: {combined_df['profile_id'].nunique()}")  # Removed, not in schema
        print(f"Total tests processed: {len(combined_df)}")
    else:
        print("No CMJ results to upload")

if __name__ == "__main__":
    # Check if credentials file exists
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"ERROR: {CREDENTIALS_FILE} not found. Please ensure your GCP credentials are in place.")
    else:
        main_pipeline() 