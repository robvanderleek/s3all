import gzip
import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from s3do.utils import do_for_all_objects


def _get_callback(bucket):
    def print_object(o):
        print(f'{bucket},{o["Key"]}')

    return print_object


def _load_from_symlink_file(client, bucket, symlink_file, func):
    obj = client.get_object(Bucket=bucket, Key=symlink_file)
    string_bytes = obj['Body'].read()
    string = bytes.decode(string_bytes)
    lines = string.split('\n')
    for line in lines:
        if line.startswith('s3://') and line.endswith('.csv.gz'):
            bucket, key = line.replace('s3://', '').split('/', 1)
            obj = client.get_object(Bucket=bucket, Key=key)
            zipped_bytes = obj['Body'].read()
            func(gzip.decompress(zipped_bytes).decode())


def inventory_command(bucket: str, prefix: str, symlink_file: str):
    try:
        client = boto3.client('s3')
        if symlink_file:
            def print_callback(data):
                print(data, end='')

            _load_from_symlink_file(client, bucket, symlink_file, print_callback)
        else:
            do_for_all_objects(client, bucket, prefix, _get_callback(bucket))
    except (ClientError, NoCredentialsError) as e:
        logging.error(e)
