from boto.s3.connection import S3Connection
from boto.dynamodb2.table import Table
from boto.dynamodb2 import connect_to_region
from twisted.internet.threads import deferToThread

_dbconn = connect_to_region("us-east-1", profile_name="dg")
_images = Table('IDAHOIngestedImages', connection=_dbconn)
_s3conn = S3Connection(profile_name="dg")
_bucket = _s3conn.get_bucket('idaho-images')

def get_idaho_metadata(idaho_id, files=['IMD','TIL']):
    return { f: _bucket.get_key('{}/{}.metadata'.format(prefix, f)).get_contents_as_string() for f in files}

def iterimages(limit=100):
    for rec in _images.scan(limit=limit):
        yield dict(rec.items())
