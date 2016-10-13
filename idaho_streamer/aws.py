from contextlib import contextmanager
from boto.s3.connection import S3Connection
from boto import connect_sqs
from boto.dynamodb2.table import Table
from boto.dynamodb2 import connect_to_region
from twisted.internet.threads import deferToThread

_dbconn = connect_to_region("us-east-1", profile_name="dg")
_images = Table('IDAHOIngestedImages', connection=_dbconn)
_s3conn = S3Connection(profile_name="dg")
_bucket = _s3conn.get_bucket('idaho-images')
_sqsconn = connect_sqs(profile_name="dg")
_queue = _sqsconn.get_queue('timbr-idaho-streaming')

def get_idaho_metadata(idaho_id, files=['IMD','TIL']):
    prefix = '{}/vendor-metadata'.format(idaho_id)
    return { f: _bucket.get_key('{}/{}.metadata'.format(prefix, f)).get_contents_as_string() for f in files}

def iterimages(limit=10):
    for rec in _images.scan(limit=limit):
        yield dict(rec.items())["IDAHO_ID"]


def next_batch():
    return _queue.get_messages(10)

def remove_batch(batch):
    for msg in batch:
        _queue.delete_message(msg)
