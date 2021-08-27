import datetime
import time
import boto3
import os

from botocore.exceptions import ClientError

# Global Variables
database = os.environ['DATABASE']
output_bucket = os.environ['OUTPUT_BUCKET']
dynamodb_table = os.environ['DYNAMODB_TABLE']
main_region = "eu-west-1"

# -------- boto3 variables -----------
athena_client = boto3.client("athena", region_name=main_region)
s3_client = boto3.client("s3", region_name=main_region)
dynamodb_client = boto3.resource("dynamodb", region_name=main_region).Table(
    dynamodb_table
)

# Get Year, Month, Day for partition
date = datetime.datetime.now()
year = str(date.year)
month = str(date.month).rjust(2, "0")
day = str(date.day).rjust(2, "0")
str_date = f"{year}-{month}-{day}"


def list_accounts(bucket_name, bucket_prefix):
    accounts = []

    response = s3_client.list_objects(
        Bucket=bucket_name, Prefix=bucket_prefix, Delimiter="/"
    )

    for account in response.get("CommonPrefixes"):
        account_id = (
            account.get("Prefix", "").replace(bucket_prefix, "").replace("/", "")
        )
        accounts.append(account_id)

    return accounts


def list_regions(bucket_name, bucket_prefix, account_id):
    regions = []

    response = s3_client.list_objects(
        Bucket=bucket_name,
        Prefix=bucket_prefix.format(account_id=account_id),
        Delimiter="/",
    )

    for region in response.get("CommonPrefixes"):
        region_name = (
            region.get("Prefix", "")
            .replace(bucket_prefix.format(account_id=account_id), "")
            .replace("/", "")
        )
        regions.append(region_name)

    return regions


def get_partition(partition_name):
    try:
        response = dynamodb_client.get_item(Key={"PartitionName": partition_name})
    except ClientError as e:
        print(e.response["Error"]["Message"])
    else:
        if "Item" in response:
            return response["Item"]


def insert_partition(partition_name):
    response = dynamodb_client.put_item(Item={"PartitionName": partition_name})
    return response


def build_query_partition(account_id, region, table_name, bucket_name, bucket_prefix):
    return str(
        "ALTER TABLE "
        + table_name
        + " ADD PARTITION (accountid='"
        + account_id
        + "',regioncode='"
        + region
        + "',`date`='"
        + str_date
        + "') location '"
        + "s3://" + bucket_name + "/"
        + bucket_prefix.format(account_id=account_id)
        + region
        + "/"
        + year
        + "/"
        + month
        + "/"
        + day
        + "';"
    )


def run_query(query, s3_output):
    query_response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={
            "OutputLocation": "s3://" + s3_output + "/",
        },
    )
    print("Execution ID: " + query_response["QueryExecutionId"])
    return query_response


def lambda_handler(event, context):
    bucket_prefix = event['bucket_prefix']
    bucket_logs_prefix = event['bucket_logs_prefix']
    bucket_name = event['bucket_name']
    table_name = event['glue_table_name']
    log_type = event['log_type']

    accounts = list_accounts(bucket_name=bucket_name, bucket_prefix=bucket_prefix)
    regions = list_regions(
        bucket_name=bucket_name,
        bucket_prefix=bucket_prefix + bucket_logs_prefix,
        account_id=accounts[0]
    )

    for account_id in accounts:
        for region in regions:
            partition_name = f"{log_type}#{account_id}#{region}#{str_date}"
            existing_partition = get_partition(
                partition_name=partition_name
            )
            if not existing_partition:
                insert_partition(partition_name=partition_name)

                query = build_query_partition(
                    account_id=account_id,
                    region=region,
                    bucket_name=bucket_name,
                    bucket_prefix=bucket_prefix + bucket_logs_prefix,
                    table_name=table_name)

                run_query(query=query, s3_output=output_bucket)
                time.sleep(0.2)

        time.sleep(1)

    response = {
        "statusCode": 200,
        "body": "Athena Partitions created"
    }

    return response
