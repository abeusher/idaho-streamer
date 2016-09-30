import os
import sys
from twisted.python import log
log.startLogging(sys.stdout)
import argparse
import datetime as dt
from itertools import chain

from dateutil.parser import parse as parse_date
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from gbdxtools import Interface
from shapely.geometry import Polygon, mapping
from shapely.wkt import loads as wkt_loads
import mercantile

from idaho_streamer.db import db, init as init_db

for key in ["GBDX_CLIENT_ID", "GBDX_CLIENT_SECRET", "GBDX_USERNAME", "GBDX_PASSWORD"]:
    assert key in os.environ

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
def populate_from_idaho_search(start_date, end_date):
    log.msg("Populating from IDAHO search...")
    images = yield deferToThread(
        gbdx.catalog.search, startDate=dt_to_ts(start_date), endDate=dt_to_ts(end_date),
        filters=["(sensorPlatformName = 'WV02' OR sensorPlatformName ='WV03')"],
        types=['IDAHOImage']
    )
    log.msg("Found {} images from {} to {}".format(len(images), start_date.isoformat(), end_date.isoformat()))
    tiles = yield deferToThread(get_idaho_tiles, images)
    log.msg("  resulting in {} footprints".format(len(tiles)))
    for tile in tiles:
        yield db.idaho_tiles.replace_one({"id": tile["id"]}, tile, upsert=True)
    log.msg("Added/Updated {} footprints".format(len(tiles)))
    returnValue(len(tiles))


def generate_footprint(img, gbdx=gbdx):
    idaho_id = img['id']
    bbox = wkt_loads(img['boundstr']).bounds
    footprint = Polygon([[bbox[0], bbox[1]],
                    [bbox[0], bbox[3]],
                    [bbox[2], bbox[3]],
                    [bbox[2], bbox[1]],
                    [bbox[0], bbox[1]]])
    obj = {
        "type": "Feature",
        "id": idaho_id,
        "properties": {
            "idahoID": idaho_id,
            "bounds": bbox,
            "center": mapping(footprint.centroid),
            "catalogId": img['catalogId'],
            "acquisitionDate": img['acquisitionDate']
        },
        "geometry": mapping(footprint),
        "_acquisitionDate": parse_date(img['acquisitionDate'])
    }
    return obj


def get_idaho_tiles(images, gbdx=gbdx):
    footprints = []
    image_props = {i['identifier']: i['properties'] for i in images}
    info = gbdx.idaho.describe_images({'results':images})
    for cid, props in info.iteritems():
        for k, part in props['parts'].iteritems():
            if 'WORLDVIEW_8_BAND' in part:
                wv8 = part['WORLDVIEW_8_BAND']
                wv8['catalogId'] = image_props[wv8['id']]['vendorDatasetIdentifier3']
                wv8['acquisitionDate'] = image_props[wv8['id']]['acquisitionDate']
                footprints.append(generate_footprint(wv8, gbdx=gbdx))
    return footprints


@inlineCallbacks
def backfill(start_date):
    yield deferToThread(refresh_gbdx)
    while start_date < dt.datetime.now():
        end_date = start_date + dt.timedelta(days=1)
        try:
            n = yield populate_from_idaho_search(start_date, end_date)
        except Exception as e:
            log.msg("Error populating footprints [{}, {}]".format(start_date.isoformat(),
                                                                       end_date.isoformat()))
            yield deferToThread(refresh_gbdx)

        start_date = start_date + dt.timedelta(days=1)


@inlineCallbacks
def poll():
    yield refresh_gbdx()
    end_date = dt.datetime.now()
    start_date = end_date - dt.timedelta(days=1)
    try:
        n = yield populate_from_idaho_search(start_date, end_date)
    except:
        # Since we're polling we don't really care.  We'll try again
        pass
    returnValue(n)


@inlineCallbacks
def run(start_date, poll_interval=3600):
    yield init_db()
    yield backfill(start_date)
    task = LoopingCall(poll)
    task.start(poll_interval)
    returnValue(task)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default='2016-01-01T00:00:00.000Z', help="timestamp from which to start polling imagery")
    args = parser.parse_args()
    start_date = parse_date(args.start)

    run(start_date)
    reactor.run()

if __name__ == '__main__':
    main()
