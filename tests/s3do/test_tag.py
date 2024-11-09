import boto3
from moto import mock_aws

from s3do.tag import _tags_to_tagset, _tag_objects


def setup_client():
    client = boto3.client('s3', 'eu-central-1')
    config = {'LocationConstraint': 'eu-central-1'}
    client.create_bucket(Bucket='Aap', CreateBucketConfiguration=config)
    s3 = boto3.resource('s3')
    bucket_versioning = s3.BucketVersioning('Aap')
    bucket_versioning.enable()
    return client


def test_tag_to_tagset_empty_set():
    result = _tags_to_tagset([])

    assert len(result) == 0


def test_tag_to_tagset_one_element():
    result = _tags_to_tagset(('aap=noot',))

    assert len(result) == 1
    assert result[0]['Key'] == 'aap'
    assert result[0]['Value'] == 'noot'


def test_tag_to_tagset_two_elements():
    result = _tags_to_tagset(('aap=noot', 'mies=wim'))

    assert len(result) == 2
    assert result[0]['Key'] == 'aap'
    assert result[0]['Value'] == 'noot'
    assert result[1]['Key'] == 'mies'
    assert result[1]['Value'] == 'wim'


@mock_aws
def test_tag_objects():
    client = setup_client()
    client.put_object(Body='Hello world', Bucket='Aap', Key='noot.txt')

    result = client.get_object_tagging(Bucket='Aap', Key='noot.txt')

    assert len(result['TagSet']) == 0

    _tag_objects(client, 'Aap', None, _tags_to_tagset(('aap=noot',)))

    result = client.get_object_tagging(Bucket='Aap', Key='noot.txt')

    assert len(result['TagSet']) == 1
