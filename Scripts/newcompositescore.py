import pandas as pd
import numpy as np

# Composite score weights for CMJ (update as needed)
CMJ_weights = {
    'CONCENTRIC_IMPULSE_Trial_Ns': 0.2,
    'ECCENTRIC_BRAKING_RFD_Trial_N_s': 0.1,
    'PEAK_CONCENTRIC_FORCE_Trial_N': 0.2,
    'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg': 0.3,
    'RSI_MODIFIED_Trial_RSI_mod': 0.1,
    'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns': 0.1
}

# Metrics where higher is worse (invert z-score)
invert_metrics = set()


def calculate_composite_score_per_trial(trial_df: pd.DataFrame, global_means, global_stds) -> pd.Series:
    """
    Given a DataFrame where rows are metrics and columns are trials, calculate the composite score for each trial.
    Returns a Series with composite scores for each trial.
    """
    # Transpose so trials are rows, metrics are columns
    trial_df = trial_df.T  # Now shape: (n_trials, n_metrics)
    z_scores = (trial_df - global_means) / global_stds
    z_scores = z_scores.reindex(columns=CMJ_weights.keys(), fill_value=np.nan)
    composite_scores = z_scores.dot(pd.Series(CMJ_weights))
    return composite_scores


def get_best_trial(trial_df: pd.DataFrame, global_means, global_stds) -> tuple:
    """
    Given a DataFrame where rows are metrics and columns are trials, returns:
    (best_trial_col, best_score, composite_scores, best_metrics_dict)
    """
    composite_scores = calculate_composite_score_per_trial(trial_df, global_means, global_stds)
    if composite_scores.isnull().all():
        return None, None, composite_scores, {}
    best_trial_col = composite_scores.idxmax()
    best_score = composite_scores[best_trial_col]
    # Extract all metrics for the best trial
    best_metrics = trial_df[best_trial_col] if best_trial_col in trial_df else pd.Series(dtype=float)
    # Ensure all required metrics are present
    best_metrics = best_metrics.reindex(list(CMJ_weights.keys()), fill_value=np.nan)
    return best_trial_col, best_score, composite_scores, best_metrics.to_dict() 
