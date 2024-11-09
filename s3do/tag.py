import logging

import boto3
import click
from botocore.exceptions import ClientError, NoCredentialsError

from s3do.utils import do_for_all_objects


def _tags_to_tagset(tags):
    result = []
    for t in tags:
        parts = t.split('=', 1)
        result.append({'Key': parts[0], 'Value': parts[1]})
    return result


def _tag_object(client, bucket, tagset, o):
    retries = 3
    while retries > 0:
        try:
            if 'VersionId' in o:
                client.put_object_tagging(
                    Bucket=bucket,
                    Key=o['Key'],
                    VersionId=o['VersionId'],
                    Tagging={
                        'TagSet': tagset
                    }
                )
            else:
                client.put_object_tagging(
                    Bucket=bucket,
                    Key=o['Key'],
                    Tagging={
                        'TagSet': tagset
                    }
                )
            return
        except Exception as e:
            print(e)
            if retries > 0:
                retries -= 1
    logging.warning('Tagging failed for object: ' + bucket + '/' + o['Key'])


def _get_callback(client, bucket, tagset):
    def callback(o):
        _tag_object(client, bucket, tagset, o)

    return callback


def _tag_objects(client, bucket, prefix, tagset):
    do_for_all_objects(client, bucket, prefix, _get_callback(client, bucket, tagset))


@click.argument('prefix', required=False)
@click.option('-i', '--symlink-file', help="Read objects from inventory file")
@click.option('-p', '--prefix')
@click.option('--tag', '-t', required=True, multiple=True)
def tag_command(bucket: str, prefix: str, symlink_file, tag):
    tagset = _tags_to_tagset(tag)
    try:
        client = boto3.client('s3')
        if symlink_file:
            with open(symlink_file) as f:
                while line := f.readline():
                    parts = line.split(',')
                    bucket = parts[0].strip().replace('"', '')
                    key = parts[1].strip().replace('"', '')
                    print(f'{bucket}/{key}')
                    _tag_object(client, bucket, tagset, {'Key': key})
        else:
            _tag_objects(client, bucket, prefix, tagset)
    except (ClientError, NoCredentialsError) as e:
        logging.error(e)
