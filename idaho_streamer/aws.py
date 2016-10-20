from contextlib import contextmanager
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto import connect_sqs
from boto.dynamodb2.table import Table
from boto.dynamodb2 import connect_to_region as db_connect_to_region
import boto3
from twisted.internet.threads import deferToThread
from twisted.internet.defer import inlineCallbacks, returnValue
from itertools import product
import hashlib
import json
import xml.etree.cElementTree as ET

from idaho_streamer.ipe import VIRTUAL_IPE_URL
from idaho_streamer.util import calc_toa_gain_offset

_dbconn = db_connect_to_region("us-east-1", profile_name="dg")
_images = Table('IDAHOIngestedImages', connection=_dbconn)
_s3conn = S3Connection(profile_name="dg")
_bucket = _s3conn.get_bucket('idaho-images')
_vrtcache = _s3conn.get_bucket('idaho-vrt')
_tilecache = _s3conn.get_bucket('idaho-lambda')
LABEL = "toa"
_sqsconn = connect_sqs(profile_name="dg")
_queue = _sqsconn.get_queue('timbr-idaho-streaming')
_session = boto3.Session(profile_name='dg')
_lambda = _session.client("lambda")


def get_idaho_metadata(idaho_id, files=['IMD','TIL']):
    prefix = '{}/vendor-metadata'.format(idaho_id)
    return { f: _bucket.get_key('{}/{}.metadata'.format(prefix, f)).get_contents_as_string() for f in files}


def iterimages():
    for rec in _images.scan():
        yield dict(rec.items())["IDAHO_ID"]


def next_batch():
    return _queue.get_messages(10)


def remove_batch(batch):
    for msg in batch:
        _queue.delete_message(msg)

DTLOOKUP = {
    "UNSIGNED_SHORT": "UInt16",
    "UNSIGNED_INT": "UInt16",
    "BYTE": "Byte",
    "FLOAT": "Float32"
}

def invoke_lambda(fname, idaho_id, z, x, y):
    cache_key = "{idaho_id}/{fname}/{z}/{x}/{y}".format(idaho_id=idaho_id, fname=fname, z=z, x=x, y=y)
    key = _tilecache.get(cache_key)
    if key is None:
        payload = {"idaho_id": idaho_id, "z": z, "x": x, "y": y}
        key = _tilecache.get(_lambda.invoke(FunctionName=fname, Payload=json.dumps(payload)), validate=False)
    return key.generate_url()

def vrt_for_id(idaho_id, meta, level=0, node="TOAReflectance"):
    # Check if vrt is in s3 vrt cache
    cache_key = "{idaho_id}/{node}/{level}.vrt".format(idaho_id=idaho_id, node=node, level=str(level))
    key = _vrtcache.get_key(cache_key)
    if key is not None:
        # If it is there, return it.
        return(key.get_contents_as_string())
    # otherwise continue
    # NOTE: validate=False saves a REST call, but involves trusting that the key exists
    if level > 0:
        rrds = json.loads(_bucket.get_key('{}/rrds.json'.format(idaho_id)).get_contents_as_string())
        try:
            idaho_id = rrds["reducedResolutionDataset"][level]["targetImage"]
        except:
            raise IndexError("Reduced level {} not available".format(level))
    md = json.loads(_bucket.get_key('{}/image.json'.format(idaho_id), validate=False).get_contents_as_string())
    warp = json.loads(_bucket.get_key('{}/native_warp_spec.json'.format(idaho_id), validate=False).get_contents_as_string())
    tfm = warp["targetGeoTransform"]
    vrt = ET.Element("VRTDataset", {"rasterXSize": str(md["tileXSize"]*md["numXTiles"]),
                                "rasterYSize": str(md["tileYSize"]*md["numYTiles"])})
    ET.SubElement(vrt, "SRS").text = tfm["spatialReferenceSystemCode"]
    # NOTE: shearX and shearY may work by coincidence or they may be incorrect terms
    ET.SubElement(vrt, "GeoTransform").text = ", ".join(map(str, [tfm["translateX"],
                                                                  tfm["scaleX"],
                                                                  tfm["shearX"],
                                                                  tfm["translateY"],
                                                                  tfm["shearY"],
                                                                  tfm["scaleY"]]))

    paths = []
    for i in xrange(md["numBands"]):
        bidx = i+1
        band = ET.SubElement(vrt, "VRTRasterBand", {"dataType": "Float32", "band": str(bidx)})
        for x, y in product(xrange(md["numXTiles"]), xrange(md["numYTiles"])):
            src = ET.SubElement(band, "ComplexSource")
            ET.SubElement(src, "SourceFilename").text = "/vsicurl/{baseurl}/{bucket}/{ipe_graph_id}/{node}/{x}/{y}".format(baseurl=VIRTUAL_IPE_URL,
                                                                                                                           buckt="idaho-images",
                                                                                                                           ipe_graph_id=meta["ipe_graph_id"],
                                                                                                                           node=node,
                                                                                                                           x=x,
                                                                                                                           y=y)
            ET.SubElement(src, "SourceBand").text =str(bidx)
            ET.SubElement(src, "SrcRect", {"xOff": str(md["tileXOffset"]), "yOff": str(md["tileYOffset"]),
                                            "xSize": str(md["tileXSize"]), "ySize": str(md["tileYSize"])})
            ET.SubElement(src, "DstRect", {"xOff": str(x*md["tileXSize"]), "yOff": str(y*md["tileYSize"]),
                                            "xSize": str(md["tileXSize"]), "ySize": str(md["tileYSize"])})

            ET.SubElement(src, "SourceProperties", {"RasterXSize": str(md["tileXSize"]), "RasterYSize": str(md["tileYSize"]),
                                                    "BlockXSize": "128", "BlockYSize": "128", "DataType": DTLOOKUP.get(md["dataType"], "Byte")})
    # cache the result
    result = ET.tostring(vrt)
    key = _vrtcache.new_key(cache_key)
    key.set_contents_from_string(result)
    return result
