import os
import sys
from twisted.python import log
log.startLogging(sys.stdout)
import argparse
import datetime as dt
from itertools import chain
import json
import hashlib

from dateutil.parser import parse as parse_date
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from gbdxtools import Interface
from shapely.geometry import Polygon, mapping
from shapely.wkt import loads as wkt_loads
import mercantile
import treq
from boto.exception import S3ResponseError

from idaho_streamer.db import db, init as init_db
from idaho_streamer.aws import get_idaho_metadata, iterimages, next_batch, remove_batch
from idaho_streamer.util import extract_idaho_metadata
from idaho_streamer.ipe import VIRTUAL_IPE_URL, generate_ipe_graph

gbdx = None
def refresh_gbdx():
    global gbdx
    gbdx = Interface(client_id=os.environ.get("GBDX_CLIENT_ID"),
                     client_secret=os.environ.get("GBDX_CLIENT_SECRET"),
                     username=os.environ.get("GBDX_USERNAME"),
                     password=os.environ.get("GBDX_PASSWORD"))
refresh_gbdx()

def dt_to_ts(dt, fmt='%Y-%m-%dT%H:%M:%S.%f'):
    s = dt.strftime(fmt)[:-3] + "Z"
    return s

@inlineCallbacks
def populate(iterable):
    for idaho_id in iterable:
        try:
            footprint = yield generate_footprint(idaho_id)
        except S3ResponseError:
            footprint = None
        if footprint is not None:
            ipe_graph = json.dumps(generate_ipe_graph(idaho_id, footprint["properties"]))
            log.msg(ipe_graph)
            digest = hashlib.md5(ipe_graph).hexdigest()
            footprint["properties"]["ipe_graph_digest"] = digest
            fprec = yield db.idaho_footprints.find_one({"id": footprint["id"]})
            if (fprec is not None) and fprec["properties"].get("ipe_graph_digest") == digest:
                ipe_id = fprec["properties"].get("ipe_graph_id")
            else:
                resp = yield treq.post("{}/graph".format(VIRTUAL_IPE_URL), ipe_graph,
                                       headers={"Content-Type": "application/json"})
                # if resp.code != 201:
                #     yield deferToThread(refresh_gbdx)
                #     resp = yield treq.post("{}/graph".format(VIRTUAL_IPE_URL), ipe_graph,
                #                            headers={"Authorization": "Bearer {}".format(gbdx.gbdx_connection.token.get("access_token")),
                #                                     "Content-Type": "application/json"})
                log.msg(resp.code)
                ipe_id = yield resp.content()
                log.msg("Created IPE Graph: {}".format(ipe_id))
            footprint["properties"]["ipe_graph_id"] = ipe_id
            yield db.idaho_footprints.replace_one({"id": footprint["id"]}, footprint, upsert=True)
            log.msg("Added/Updated record for Idaho Id: {}".format(idaho_id))
        else:
            log.msg("Ignored Idaho Id: {}".format(idaho_id))


@inlineCallbacks
def generate_footprint(idaho_id):
    try:
        md_files = yield deferToThread(get_idaho_metadata, idaho_id)
    except AttributeError:
        returnValue(None)
    metadata = extract_idaho_metadata(md_files['IMD'], md_files['TIL'])
    if metadata['satid'] in ['WV02', 'WV03'] and metadata['bandid'] == 'Multi':
        bbox = metadata['bbox']
        footprint = Polygon([[bbox[0], bbox[1]],
                    [bbox[0], bbox[3]],
                    [bbox[2], bbox[3]],
                    [bbox[2], bbox[1]],
                    [bbox[0], bbox[1]]])
        obj = {
            "type": "Feature",
            "id": idaho_id,
            "properties": metadata,
            "geometry": mapping(footprint),
            "_acquisitionDate": metadata['img_datetime_obj_utc']
        }
        obj['properties']['center'] = mapping(footprint.centroid)
        returnValue(obj)


@inlineCallbacks
def backfill():
    yield populate(iterimages())

@inlineCallbacks
def fixmissing():
    docs, d = yield db.idaho_footprints.find({}, fields={"_acquisitionDate": False}, cursor=True)

    while docs:
        pending = [doc["id"] for doc in docs if "ipe_graph_id" not in doc["properties"]]
        yield populate(pending)
        docs, d = yield d

@inlineCallbacks
def poll():
    more = True
    while more:
        batch = yield deferToThread(next_batch)
        if len(batch) < 10:
            more = False
        ids = [json.loads(json.loads(rec.get_body())["Message"])["identifier"] for rec in batch]
        yield populate(ids)
        yield deferToThread(remove_batch, batch)


@inlineCallbacks
def run(interval):
    yield init_db()
    # yield backfill()
    # log.msg("Done Backfilling.  Starting Live Stream...")
    yield fixmissing()
    task = LoopingCall(poll)
    task.start(interval)
    returnValue(task)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--interval", default=10.0, help="SQS polling interval in seconds")
    args = parser.parse_args()

    run(args.interval)
    reactor.run()

if __name__ == '__main__':
    main()
