# api.py
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

load_dotenv()
FORCEDECKS_URL = os.getenv("FORCEDECKS_URL")
DYNAMO_URL = os.getenv("DYNAMO_URL")
PROFILE_URL = os.getenv("PROFILE_URL")
TENANT_ID = os.getenv("TENANT_ID")

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
AUTH_URL = os.getenv("AUTH_URL")
CACHE_FILE = ".token_cache.json"

def get_access_token():
    # Check cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
            if datetime.now() < datetime.fromisoformat(data["expires_at"]):
                return data["access_token"]

    # Generate new token
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    response = requests.post(AUTH_URL, data=payload)
    if response.status_code == 200:
        token = response.json()['access_token']
        expires_in = response.json().get('expires_in', 7200)
        expires_at = (datetime.now() + timedelta(seconds=expires_in - 60)).isoformat()

        with open(CACHE_FILE, "w") as f:
            json.dump({"access_token": token, "expires_at": expires_at}, f)

        print("Access token refreshed.")
        return token
    else:
        raise Exception(f"Auth failed: {response.status_code} - {response.text}")


def get_profiles(token):
    today = datetime.today()
    url = f"{PROFILE_URL}/profiles?tenantId={TENANT_ID}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        df = pd.DataFrame(response.json()['profiles'])
        df['givenName'] = df['givenName'].str.strip()
        df['familyName'] = df['familyName'].str.strip()
        df['fullName'] = df['givenName'] + ' ' + df['familyName']
        df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'])
        df['age'] = (
            today.year - df['dateOfBirth'].dt.year -
            ((today.month < df['dateOfBirth'].dt.month) |
             ((today.month == df['dateOfBirth'].dt.month) & (today.day < df['dateOfBirth'].dt.day)))
        ).astype(int)
        return df
    else:
        print(f"Failed to get profiles: {response.status_code}")
        return pd.DataFrame()
    

def FD_Tests_by_Profile(DATE, profileId, token):
    url=f"{FORCEDECKS_URL}/tests?TenantId={TENANT_ID}&ModifiedFromUtc={DATE}&ProfileId={profileId}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        df = pd.DataFrame(response.json()['tests'])
        df = df[['testId', 'modifiedDateUtc', 'testType']]
        print(response.status_code)
        return df
    else:
        print(response.status_code)

def unit_map(unit: str) -> str:
    _map = {
        'Centimeter':                       'cm',
        'Inch':                             'in',
        'Joule':                            'J',
        'Kilo':                             'kg',
        'Meter Per Second':                 'm/s',
        'Meter Per Second Per Second':     'm/s²',
        'Millisecond':                      'ms',
        'Second':                           's',
        'Newton':                           'N',
        'Newton Per Centimeter':            'N/cm',
        'Newton Per Kilo':                  'N/kg',
        'Newton Per Meter':                 'N/m',
        'Newton Per Second':                'N/s',
        'Newton Per Second Per Centimeter': 'N/s/cm',
        'Newton Per Second Per Kilo':       'N/s/kg',
        'Newton Second':                    'Ns',
        'Newton Second Per Kilo':           'Ns/kg',
        'Watt':                             'W',
        'Watt Per Kilo':                    'W/kg',
        'Watt Per Second':                  'W/s',
        'Watt Per Second Per Kilo':         'W/s/kg',
        'Joule':                            'J',
        'Percent':                          '%',
        'Pound':                            'lb',
        'RSIModified':                      'RSI_mod',
        'No Unit':                          '',     # blank for unitless
    }
    return _map.get(unit, unit)

def get_FD_results(testId, token):
    url = f"{FORCEDECKS_URL}/v2019q3/teams/{TENANT_ID}/tests/{testId}/trials"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        test_data = response.json()
        if not test_data or not isinstance(test_data, list):
            print("Unexpected response format")
            return None

        all_results = []
        for trial in test_data:
            results = trial.get("results", [])
            for res in results:
                flat_result = {
                    "resultId": res.get("resultId"),
                    "value": res.get("value"),
                    "time": res.get("time"),
                    "limb": res.get("limb"),
                    "repeat": res.get("repeat"),
                    "definition_id": res["definition"].get("id"),
                    "result_key": res["definition"].get("result"),
                    "description": res["definition"].get("description"),
                    "name": res["definition"].get("name"),
                    "unit": res["definition"].get("unit"),
                    "repeatable": res["definition"].get("repeatable"),
                    "asymmetry": res["definition"].get("asymmetry")
                }
                all_results.append(flat_result)

        df = pd.DataFrame(all_results)
        df['unit'] = df['unit'].apply(unit_map)
        df['metric_id'] = (df['result_key'].astype(str) + '_' + df['limb'].astype(str) + '_' + df['unit'])
        # Make metric_id BigQuery-safe by replacing '/' with '_' and removing trailing underscores
        df['metric_id'] = df['metric_id'].str.replace('/', '_', regex=False)
        df['metric_id'] = df['metric_id'].str.rstrip('_')
        df['trial'] = df.groupby('metric_id').cumcount() + 1
        pivot = df.pivot(index='metric_id', columns='trial', values='value')
        pivot.columns = [f'trial {c}' for c in pivot.columns]
        pivot = pivot.reset_index()
        df = pivot
        df.to_csv('test_results.csv', index=False)
        return df
    else:
        print(response.status_code)

def get_dynamo_results(profileId, token):
    url = f"{DYNAMO_URL}/v2022q2/teams/{TENANT_ID}/tests?athleteId={profileId}&includeRepSummaries=false&includeReps=false"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        return df
    else:
        print(response.status_code)

