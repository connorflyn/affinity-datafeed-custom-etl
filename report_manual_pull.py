# Import Packages
import requests
import boto3
import time
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import os
import argparse
from dotenv import load_dotenv
from pathlib import Path

# Import Credntials
base_path = os.getcwd()
dotenv_path = Path(base_path + r'/.env')
load_dotenv(dotenv_path=dotenv_path)

# Affinity Credentials
affinity_client_id = os.getenv('AFFINITY_CLIENT_ID')
affinity_client_secret = os.getenv('AFFINITY_CLIENT_SECRET')

# AWS Credentials
aws_secret_access_key = os.getenv('AWS_AWS_SECRET_ACCESS_KEY')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
s3_role = os.getenv('AWS_S3_ROLE')
s3_bucket = os.getenv('AWS_S3_BUCKET')

# Redshift Credentials
redshift_host = os.getenv('REDSHIFT_HOST')
redshift_username = os.getenv('REDSHIFT_USERNAME')
redshift_port = os.getenv('REDSHIFT_PORT')
redshift_database = os.getenv('REDSHIFT_DATABASE')
redshift_password = os.getenv('REDSHIFT_PASSWORD')
redshift_schema = os.getenv('REDSHIFT_SCHEMA')
redshift_iam_role = os.getenv('REDSHIFT_IAM_ROLE')

# Redshift configuration settings
schema_name = "affinity_datafeed"
table_name = "report"

# Affinity Brand ID list
brand_id_list = "[1144517, 1144201, 1141779, 1144204, 1144200, 1138813, 1141408, 1144203, 1140723," \
                " 1142948, 1144205, 1141160, 1129054, 1144202, 1103436, 1144206]"
insight_run_time = datetime.now()

# Add shell option
parser = argparse.ArgumentParser()

parser.add_argument("-d", "--days_ago", help="Report start date (end date will be current date)")
parser.add_argument("-s", "--start_date", help="start date in format YYYYMMDD")
parser.add_argument("-e", "--end_date", help="end date in format YYYYMMDD")
parser.add_argument("-n", "--insights_name", help="Report name")
parser.add_argument("-id", "--insights_id", help="Insight ID")
parser.add_argument("-b", "--breakout_interval", help="Report grouping by; weekly, monthly, yearly")


# Define Function to copy data to S3
def copy_to_s3(client, df, bucket, file_path):
    # print("[INFO] "+dir(client))
    """using boto3 client object, upload data-frame to s3 bucket"""
    csv_buf = StringIO()
    # print(f'\n[INFO] csv_buf:{csv_buf}\n')
    print("[INFO] Dataframe to be sent to S3:\n'", df)
    # df.to_json(csv_buf)
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    # print('[INFO] S3 upload body:', csv_buf.getvalue())
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=file_path)
    print(f'[INFO] Copy {df.shape[0]} rows to S3 Bucket {bucket} at {file_path}, Done!')
    return f'[INFO] S3 path: s3://{bucket}/{file_path}'


# Define function to load pandas dataframe to S3 Bucket
def load(df, bucket, schema_name, table_name, run_time):
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=s3_role,
        RoleSessionName="AssumeRoleSession1"
    )

    role_credentials = assumed_role_object['Credentials']

    s3_resource = boto3.client(
        's3',
        aws_access_key_id=role_credentials['AccessKeyId'],
        aws_secret_access_key=role_credentials['SecretAccessKey'],
        aws_session_token=role_credentials['SessionToken'],
    )

    file_path = f'ruggable/{schema_name}/us/raw/{table_name}/export_time={run_time}.csv'

    copy_to_s3(s3_resource, df, bucket, file_path)











# Define Run Post Request Function(to create the report)
def run_bearer_token_query(query):
    # print(f'\n[INFO] Post request HEADER: {bearer_token_headers} \n')
    # print(f'\n[INFO] Post request JSON/Body: {query} \n')

    response = requests.post(url="https://api.affinitymarketingcloud.com/oauth/client_credential/accesstoken?"
                                "grant_type=client_credentials",
                             data=query,
                             headers=bearer_token_headers)

    # checks status code ... if successful (200), then returns json object
    if response.status_code == 200:

        return response

    else:
        print('\n')
        print(f'[WARNING] {response.text}')
        raise Exception(f'\n\nquery failed to run by returning code of '
                        f'{response.status_code}. '
                        f'\nAnd the query is: {query}\n')


# Define Run Post Request Function(to create the report)
def run_datafeed_report_query(query):
    # print(f'\n[INFO] Post request HEADER: {datafeed_brands_headers} \n')
    # print(f'\n[INFO] Post request JSON/Body: {query} \n')

    print(f'\n[INFO] Running Post Request: {query} \n')
    response = requests.post(url="https://api.affinitymarketingcloud.com/v3/datafeed",
                             data=query,
                             headers=datafeed_brands_headers)

    # checks status code ... if successful (200), then returns json object
    if response.status_code == 200:

        return response

    else:
        print('\n')
        print(f'[WARNING] {response.text}')
        raise Exception(f'\n\nquery failed to run by returning code of '
                        f'{response.status_code}. '
                        f'\nAnd the query is: {query}\n')


# Define Run Post Request Function(to create the report)
def run_datafeed_get_query(url):
    # print(f'\n[INFO] Post request HEADER: {datafeed_brands_headers} \n')
    # print(f'\n[INFO] Post request JSON/Body: {query} \n')

    print(f'\n[INFO] Running GET Request... \n')
    response = requests.get(url=url,
                            headers=datafeed_brands_headers)

    # checks status code ... if successful (200), then returns json object
    if response.status_code == 200:

        return response

    else:
        print('\n')
        print(f'[WARNING] {response.text}')
        raise Exception(f'\n\nquery failed to run by returning code of '
                        f'{response.status_code}. '
                        f'\nAnd the query is: {url}\n')


# Create Specific Brand(s) Weekly Reports

# Check for date commands (if none give the start date is a week ago, end date is yesterday)
try:
    args = parser.parse_args()

    if not args.start_date:
        start_date = datetime.strftime(datetime.now() - timedelta(int(8)), '%Y%m%d')
        end_date = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
    else:
        start_date = str(args.start_date)
        end_date = str(args.end_date)
except Exception as e:
    print(str(e))
    raise
# Check for an Insight Name command (if none is give, insight_name is set to "datafeed_brands")
try:
    args = parser.parse_args()

    if not args.insights_name:
        insights_name = f"datafeed_brands_run_time={insight_run_time}"
    else:
        insights_name = str(args.insights_name)
except Exception as e:
    print(str(e))
    raise
# Check for an Insight Breakout command (if none is give, breakout_interval is set to "datafeed_brands")
try:
    args = parser.parse_args()

    if not args.breakout_interval:
        breakout_interval = "weekly"
    else:
        breakout_interval = str(args.breakout_interval)
except Exception as e:
    print(str(e))
    raise


# Create Post Request Header
bearer_token_headers = {"Content-Type": "application/x-www-form-urlencoded"}

# Create Post Request Query
bearer_token_query = "client_id="+affinity_client_id+"&" \
            "client_secret="+affinity_client_secret+""

bearer_token_response = run_bearer_token_query(bearer_token_query)
bearer_token_response = run_bearer_token_query(bearer_token_query)
bearer_token_query_response = bearer_token_response.json()
bearer_token = bearer_token_query_response["access_token"]
print(f'\n[INFO] JSON bearer_token_query_response is:\n {bearer_token}\n')
# print(f'\n[INFO] JSON bearer_token is: {bearer_token}\n')


# Create Post Request Header
datafeed_brands_headers = {"Authorization": "Bearer " + bearer_token, "Content-Type": "application/json "}


# Create the Response Insight_ID variable (response_insight_id) - used to pull s3 file
args = parser.parse_args()
response_insight_id = args.insights_id
print(f'[INFO] JSON datafeed_brands_response "insights_id" is: {response_insight_id}')

datafeed_get_url = f"https://api.affinitymarketingcloud.com/v3/datafeed/{response_insight_id}/output"

print(f'[INFO] datafeed_get_url is: {datafeed_get_url}\n')

# Run Get request & transform to JSON format
datafeed_get_query_response = run_datafeed_get_query(datafeed_get_url)
datafeed_get_query_response = datafeed_get_query_response.json()
print(f'[INFO] JSON datafeed_get_query_response is:\n {datafeed_get_query_response}\n')

# Denest Get request
datafeed_get_data_param = datafeed_get_query_response["data"]
datafeed_get_results_param = datafeed_get_data_param["results"]
print(f'\n[INFO] JSON datafeed_get_results_param is: {datafeed_get_results_param}')
datafeed_get_results_downloads_param = datafeed_get_data_param["result_downloads"]
print(f'[INFO] JSON datafeed_get_results_downloads_param is: {datafeed_get_results_downloads_param}\n')

# Transform Affinity CSV file to Pandas Dataframe
report_pandas_df = pd.read_csv(str(datafeed_get_results_param))
print(f'[INFO] pandas data frame (transformed from CSV download):\n\n{report_pandas_df}')
# Load Data
print(f'\n[INFO] Starting Load (DF > S3 Bucket > Redshift Table ({schema_name}.{table_name})...\n\n')
load(report_pandas_df, s3_bucket, schema_name, table_name, insight_run_time)
print('\n[SUCCESS] Finished running')

