from pymongo import MongoClient

from shapely.wkt import loads
import mercantile
from gbdxtools import Interface

import datetime 
from dateutil import parser
import time
import simplejson as json

from requests.exceptions import ConnectionError

def find_center(bounds):
    return ( (bounds[0] + bounds[2])/ 2, (bounds[1] + bounds[3])/ 2);

def most_recent_adt(coll):
    res = coll.find()
    res.skip(res.count() - 1)
    doc = res.next()
    return doc['_acquisitionDate']

def nudge_datetime(dt):
    return dt + datetime.timedelta(microseconds=1000)

def dt_to_ts(dt, fmt='%Y-%m-%dT%H:%M:%S.%f'):
    s = dt.strftime(fmt)[:-3] + "Z"
    return s

def collect_tiles(img, zoom):
    bounds = loads(img['properties']['footprintWkt']).bounds
    img_tiles = []
    tiles = mercantile.tiles(bounds[0], bounds[1], bounds[2], bounds[3], zooms=[zoom])
    for i,t in enumerate(tiles):
        bnds = mercantile.bounds(t)
        bbox = (bnds.west, bnds.south, bnds.east, bnds.north)
        obj = {
            "_acquisitionDate": parser.parse(img['properties']['acquisitionDate']),
            "type": "Feature",
            "identifier": img['identifier'],
            "properties": {
                "value": 100,
                "zxy": (t.z, t.x, t.y),
                "bounds": bbox,
                "center": find_center(bbox),
                "catalogId": img['properties']['vendorDatasetIdentifier3'],
                "acquisitionDate": img['properties']['acquisitionDate']
            },
            "geometry": {
                "type": "Point",
                "coordinates": find_center(bbox)
            }
        }
        img_tiles.append(obj)
    return img_tiles

class IdahoScraper(object):
    def __init__(self, start_time, creds, mongo_host='192.168.99.100', mongo_port=27017, 
                    search_filters=["(sensorPlatformName = 'WV02' OR sensorPlatformName ='WV03')"], 
                    database='idaho', collection='tiles', polling_interval=600, zoom=12):
        self.start_time = start_time

        self._db_name = database
        self._coll_name = collection
        self._creds = creds
        self.mongo = MongoClient(":".join([mongo_host, str(mongo_port)]))
        self.db = self.mongo[self._db_name]
        self.coll = self.db[self._coll_name]
        self.polling_interval = polling_interval
        self.filters = search_filters
        self.zoom = zoom
        self.gb_connect()
        self.setup()

    def gb_connect(self):
        self.gbdx = Interface(**self._creds)

    @property
    def start(self):
        return dt_to_ts(self.since)

    @property
    def end(self):
        return dt_to_ts(self.until)

    def setup(self):
        if self.coll.count() > 0:
            self.since = nudge_datetime(most_recent_adt(self.coll))
        else:
            self.since = parser.parse(self.start_time)
        self.until = self.since + datetime.timedelta(days=7)

    def is_unique(self, search_params):
        cur = self.coll.find(search_params)
        cnt = cur.count()
        cur.close()
        if cnt > 0:
            return False
        return True

    def idaho_search(self):
        res  = self.gbdx.catalog.search(startDate=self.start, endDate=self.end, filters=self.filters, types=['IDAHOImage'])
        filtered = [r for r in res if self.is_unique({"identifier": r['identifier']})]
        return filtered

    def get_idaho_tiles(self, images):
        tiles = []
        for img in images:
            tiles += self.collect_tiles(img)
        return tiles

    def update(self):
        print("old params {} {}".format(self.start, self.end))
        self.since = nudge_datetime(self.until)
        self.until = self.since + datetime.timedelta(days=7)
        print("new params {} {}".format(self.start, self.end))

    def poll(self):
        while True:
            results = []
            try:
                results = self.idaho_search()
            except ConnectionError as ce:
                self.gb_connect()
            if len(results) > 0:
                tiles = []
                print("processing {} images".format(len(results)))
                for img in results:
                    tiles += collect_tiles(img, self.zoom)
                print("about to insert {}".format(len(tiles)))
                self.coll.insert_many(tiles)
                print("successfully inserted {}".format(len(tiles)))
                self.update()
            else:
                print("sleeping")
                time.sleep(self.polling_interval)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--credpath", help="filepath to json file with gbdx creds")
    parser.add_argument("--mongo-host", default='192.168.99.100', help="mongo host address")
    parser.add_argument("--mongo-port", type=int, default=27017, help="mongo port number")
    parser.add_argument("--start", default='2016-09-15T01:46:53.568Z', help="timestamp from which to start polling imagery")
    args = parser.parse_args() 

    #parser.add_argument("--start", default='/Users/jamiepolackwich1/dockerfiles/idaho-scraper/.creds.json')

    with open(args.credpath, 'r') as f:
        creds = json.load(f)

    scraper = IdahoScraper(args.start, creds, mongo_host=args.mongo_host, mongo_port=args.mongo_port)
    scraper.poll()

if __name__ == '__main__':
    main()
