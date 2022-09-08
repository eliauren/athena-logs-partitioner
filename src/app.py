import os
import datetime
import time
import boto3


from typing import List

# Global Variables
MAIN_REGION = os.environ["AWS_REGION"]
DATABASE = os.environ['DATABASE']
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
DYNAMODB_TABLE = os.environ['DYNAMODB_TABLE']

# -------- boto3 variables -----------
athena = boto3.client('athena', region_name=MAIN_REGION)
s3 = boto3.client('s3', region_name=MAIN_REGION)
dynamodb_table = boto3.resource('dynamodb', region_name=MAIN_REGION).Table(DYNAMODB_TABLE)

# Get Year, Month, Day for partition
DATE = datetime.datetime.now()
YEAR = str(DATE.year)
MONTH = str(DATE.month).rjust(2, '0')
DAY = str(DATE.day).rjust(2, '0')
STR_DATE = f'{YEAR}-{MONTH}-{DAY}'


def list_accounts(bucket_name: str, bucket_prefix: str) -> List[str]:
    common_prefixes = s3.list_objects(
        Bucket=bucket_name, Prefix=bucket_prefix, Delimiter='/'
    )['CommonPrefixes']

    return [
        account.get('Prefix', '').replace(bucket_prefix, '').replace('/', '')
        for account in common_prefixes
    ]


def list_regions(bucket_name: str, bucket_prefix: str, account_id: str) -> List[str]:
    common_prefixes = s3.list_objects(
        Bucket=bucket_name, Prefix=bucket_prefix.format(account_id=account_id), Delimiter='/'
    )['CommonPrefixes']

    return [
        region.get('Prefix', '').replace(bucket_prefix.format(account_id=account_id), '').replace('/', '')
        for region in common_prefixes
    ]


def is_partition_exist(partition_name: str) -> bool:
    response = dynamodb_table.get_item(
        Key={'PartitionName': partition_name}
    )
    return 'Item' in response


def insert_partition(partition_name: str):
    dynamodb_table.put_item(
        Item={'PartitionName': partition_name}
    )


def build_query_partition(
        account_id: str,
        region: str,
        table_name: str,
        bucket_name: str,
        bucket_prefix: str
) -> str:
    prefix_formatted = bucket_prefix.format(account_id=account_id)

    return f"""
        ALTER TABLE {table_name}
        ADD PARTITION (
            accountid='{account_id}',
            regioncode='{region}',
            `date`='{STR_DATE}
            )
        location 's3://{bucket_name}/{prefix_formatted}{region}/{YEAR}/{MONTH}/{DAY}';
        """


def run_query(query: str, s3_output: str):
    query_response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': f"s3://{s3_output}/"}
    )

    print(f"Execution ID: {query_response['QueryExecutionId']}")
    return query_response


def create_partition(
        partition_name: str,
        account_id: str,
        region: str,
        bucket_name: str,
        bucket_prefix: str,
        table_name: str
):
    existing_partition = is_partition_exist(partition_name=partition_name)

    if existing_partition:
        print(f"Partition {partition_name} already exists")
        return

    query = build_query_partition(
        account_id=account_id,
        region=region,
        bucket_name=bucket_name,
        bucket_prefix=bucket_prefix,
        table_name=table_name
    )

    run_query(query=query, s3_output=OUTPUT_BUCKET)
    insert_partition(partition_name=partition_name)

    # Sleep to avoid rate limiting
    time.sleep(0.3)


def get_partitions_to_create(accounts: List[str], regions: List[str], log_type: str):

    partition_to_create = []

    for account_id in accounts:
        for region in regions:
            partition_to_create.append({
                "partition_name": f"{log_type}#{account_id}#{region}#{STR_DATE}",
                "account_id": account_id,
                "region": region,
                "hour": None
            })
    return partition_to_create


def lambda_handler(event: dict, context):
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

    partitions_to_create = get_partitions_to_create(
        accounts=accounts,
        regions=regions,
        log_type=log_type
    )

    for partition in partitions_to_create:
        create_partition(
            partition_name=partition["partition_name"],
            account_id=partition["account_id"],
            region=partition["region"],
            bucket_name=bucket_name,
            bucket_prefix=bucket_prefix + bucket_logs_prefix,
            table_name=table_name
        )

    time.sleep(1)

    print(f'Partitions Created {len(partitions_to_create)}', partitions_to_create)
    return partitions_to_create
