"""
Enhanced CMJ Processor with Composite Scoring Integration
This script integrates composite scoring into the existing GCP pipeline for CMJ data.
"""

import pandas as pd
import uuid
from newcompositescore import calculate_composite_score_per_trial, get_best_trial, CMJ_weights
from VALDapiHelpers import get_access_token, get_profiles, FD_Tests_by_Profile, get_FD_results
from config import settings
from bigquery_helpers import upload_to_bigquery, bq_client
from logging_utils import get_logger

# Configuration
TABLE_ID = settings.gcp.cmj_table_id
logger = get_logger(__name__)

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
    logger.info("Fetching CMJ data for test %s...", test_id)
    raw_data = get_FD_results(test_id, token)

    if raw_data is None or raw_data.empty:
        logger.info("No data found for test %s", test_id)
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
        logger.info("No CMJ metrics found for test %s", test_id)
        return None, None
    
    # Pivot data to get trials as columns
    trial_cols = [col for col in cmj_data.columns if 'trial' in col.lower()]
    if not trial_cols:
        logger.info("No trial data found for test %s", test_id)
        return None, None
    
    # Create pivot table with metrics as index and trials as columns
    pivot_data = cmj_data.set_index('metric_id')[trial_cols]
    if not isinstance(pivot_data, pd.DataFrame):
        pivot_data = pd.DataFrame(pivot_data)
    # DEBUG: Print available metric names
    logger.debug("Available metrics in pivot_data for test %s: %s", test_id, list(pivot_data.index))
    # Calculate composite scores per trial
    best_trial_col, best_score, composite_scores, best_metrics = get_best_trial(pivot_data)
    if best_trial_col is None:
        logger.info("No valid composite scores for test %s", test_id)
        return None, None
    # DEBUG: Print best_metrics dict
    logger.debug("best_metrics for test %s: %s", test_id, best_metrics)
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
        logger.info("No tests found for profile %s", profile_id)
        return []
    
    # Filter for CMJ tests
    cmj_tests = tests_df[tests_df['testType']== 'CMJ']
    
    if cmj_tests.empty:
        logger.info("No CMJ tests found for profile %s", profile_id)
        return []
    
    logger.info("Found %d CMJ tests for profile %s", len(cmj_tests), profile_id)
    
    processed_results = []
    
    for _, test_row in cmj_tests.iterrows():
        test_id = test_row['testId']
        test_date = pd.to_datetime(test_row['modifiedDateUtc']).date()

        logger.info("Processing CMJ test %s from %s...", test_id, test_date)

        # Process the test
        gcp_data, gcp_schema = process_cmj_test_with_composite(test_id, token, assessment_id)

        # Only append if gcp_data is a valid, non-empty DataFrame
        if isinstance(gcp_data, pd.DataFrame) and not gcp_data.empty:
            gcp_data['athlete_name'] = athlete_name
            processed_results.append(gcp_data)
            logger.info("Successfully processed test %s", test_id)
        else:
            logger.warning("Failed to process test %s", test_id)
    
    return processed_results

def main_pipeline():
    """
    Main pipeline to process CMJ data with composite scoring for all athletes.
    """

    # Ensure BigQuery client is available
    if bq_client is None:
        logger.error("BigQuery client not available. Exiting.")
        return
    
    # Get access token
    token = get_access_token()
    if not token:
        logger.error("Failed to get access token")
        return
    
    # Fetch all athlete profiles
    logger.info("Fetching athlete profiles...")
    profiles = get_profiles(token)
    if profiles.empty:
        logger.info("No profiles found. Exiting.")
        return
    
    logger.info("Found %d athlete profiles", len(profiles))
    
    # Only process the first 10 athletes
    profiles = profiles.head(10)
    
    # Process each athlete
    all_results = []
    
    for index, athlete in profiles.iterrows():
        profile_id = str(athlete['profileId'])
        athlete_name = str(athlete['fullName'])
        
        logger.info("Processing athlete %d/%d: %s", index + 1, len(profiles), athlete_name)
        
        # Create assessment ID for this athlete
        assessment_id = str(uuid.uuid4())
        
        # Process all CMJ tests for this athlete
        athlete_results = process_all_cmj_tests_for_athlete(profile_id, token, assessment_id, athlete_name)
        
        if athlete_results:
            all_results.extend(athlete_results)
            logger.info("Processed %d CMJ tests for %s", len(athlete_results), athlete_name)
        else:
            logger.info("No CMJ tests processed for %s", athlete_name)
        
        # Add delay to avoid rate limiting
        if index < len(profiles) - 1:
            logger.info("Waiting 2 seconds before next athlete...")
            import time
            time.sleep(2)
    
    # After collecting all_results and before upload
    if all_results:
        logger.info("Uploading %d total CMJ results to BigQuery...", len(all_results))
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
        logger.debug("Columns in combined_df before upload: %s", combined_df.columns.tolist())
        logger.debug("First 5 rows of combined_df:")
        logger.debug("%s", combined_df.head())
        # Upload all columns (including metrics and composite score)
        upload_to_bigquery(combined_df, TABLE_ID)
        
        # Print summary statistics
        logger.info("Summary Statistics:")
        logger.info("Average Composite Score: %.3f", combined_df["cmj_composite_score"].mean())
        logger.info("Best Composite Score: %.3f", combined_df["cmj_composite_score"].max())
        # print(f"Number of athletes with data: {combined_df['profile_id'].nunique()}")  # Removed, not in schema
        logger.info("Total tests processed: %d", len(combined_df))
    else:
        logger.info("No CMJ results to upload")

if __name__ == "__main__":
    main_pipeline()