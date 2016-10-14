from contextlib import contextmanager
from boto.s3.connection import S3Connection
from boto import connect_sqs
from boto.dynamodb2.table import Table
from boto.dynamodb2 import connect_to_region
from twisted.internet.threads import deferToThread
from twisted.internet.defer import inlineCallbacks, returnValue
from itertools import product
import hashlib
import json
import xml.etree.cElementTree as ET

from idaho_streamer.util import calc_toa_gain_offset

_dbconn = connect_to_region("us-east-1", profile_name="dg")
_images = Table('IDAHOIngestedImages', connection=_dbconn)
_s3conn = S3Connection(profile_name="dg")
_bucket = _s3conn.get_bucket('idaho-images')
_sqsconn = connect_sqs(profile_name="dg")
_queue = _sqsconn.get_queue('timbr-idaho-streaming')


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


def vrt_for_id(idaho_id, meta):
    md = json.loads(_bucket.get_key('{}/image.json'.format(idaho_id)).get_contents_as_string())
    warp = json.loads(_bucket.get_key('{}/native_warp_spec.json'.format(idaho_id)).get_contents_as_string())
    gains_offsets = calc_toa_gain_offset(meta)
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
            formatted_string = "{}-{}-{}".format(idaho_id, x, y)
            hashed_string = hashlib.md5(formatted_string).hexdigest()
            prefix = hashed_string[:4]
            path = "{partition}{prefix}/{idaho_id}/{x}/{y}.{fmt}".format(partition=md["tilePartition"], prefix=prefix, hashed_string=hashed_string,
                                                                                        idaho_id=idaho_id, x=x, y=y, fmt=md["nativeTileFileFormat"].lower())
            src = ET.SubElement(band, "ComplexSource")
            ET.SubElement(src, "SourceFilename").text = "/vsis3/idaho-images/{}".format(path)
            ET.SubElement(src, "SourceBand", text=str(bidx))
            ET.SubElement(src, "SrcRect", {"xOff": str(md["tileXOffset"]), "yOff": str(md["tileYOffset"]),
                                            "xSize": str(md["tileXSize"]), "ySize": str(md["tileYSize"])})
            ET.SubElement(src, "DstRect", {"xOff": str(x*md["tileXSize"]), "yOff": str(y*md["tileYSize"]),
                                            "xSize": str(md["tileXSize"]), "ySize": str(md["tileYSize"])})


            ET.SubElement(src, "ScaleOffset").text = str(gains_offsets[i][1])
            ET.SubElement(src, "ScaleRatio").text = str(gains_offsets[i][0])
    returnValue(ET.tostring(vrt))
