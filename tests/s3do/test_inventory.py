import gzip

import boto3
from moto import mock_aws

from s3do.inventory import _load_from_symlink_file


def setup_client():
    client = boto3.client('s3', 'eu-central-1')
    config = {'LocationConstraint': 'eu-central-1'}
    client.create_bucket(Bucket='Aap', CreateBucketConfiguration=config)
    s3 = boto3.resource('s3')
    bucket_versioning = s3.BucketVersioning('Aap')
    bucket_versioning.enable()
    client.put_object(Body='s3://Aap/aap.csv.gz\ns3://Aap/noot.csv.gz\n', Bucket='Aap', Key='symlink.txt')
    client.put_object(Body=gzip.compress(bytes('"Aap","aap.txt"\n', 'utf-8')), Bucket='Aap', Key='aap.csv.gz')
    client.put_object(Body=gzip.compress(bytes('"Aap","noot.txt"\n', 'utf-8')), Bucket='Aap', Key='noot.csv.gz')
    return client


@mock_aws
def test_inventory():
    client = setup_client()

    result = ''

    def append(data):
        nonlocal result
        result += data

    _load_from_symlink_file(client, 'Aap', 'symlink.txt', append)

    assert result == '"Aap","aap.txt"\n"Aap","noot.txt"\n'
