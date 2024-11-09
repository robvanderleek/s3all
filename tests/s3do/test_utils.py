import boto3
from moto import mock_aws

from s3do.utils import do_for_all_objects


def setup_client():
    client = boto3.client('s3', 'eu-central-1')
    config = {'LocationConstraint': 'eu-central-1'}
    client.create_bucket(Bucket='Aap', CreateBucketConfiguration=config)
    return client


@mock_aws
def test_do_for_all_objects_empty_bucket():
    client = setup_client()
    called = 0

    def callback():
        nonlocal called
        called += 1

    do_for_all_objects(client, 'Aap', None, callback)

    assert called == 0


@mock_aws
def test_do_for_all_objects_one_object():
    client = setup_client()
    client.put_object(Body='Hello world', Bucket='Aap', Key='noot.txt')

    called = 0

    def callback(_):
        nonlocal called
        called += 1

    do_for_all_objects(client, 'Aap', None, callback)

    assert called == 1


@mock_aws
def test_do_for_all_objects_two_objects():
    client = setup_client()
    client.put_object(Body='Hello world', Bucket='Aap', Key='noot.txt')
    client.put_object(Body='Hello world', Bucket='Aap', Key='mies/wim/zus.txt')

    called = 0

    def callback(_):
        nonlocal called
        called += 1

    do_for_all_objects(client, 'Aap', None, callback)

    assert called == 2
