from pymongo import MongoClient

from shapely.wkt import loads
import mercantile
from gbdxtools import Interface

import datetime 
import dateutil
import time
    
def find_center(bounds):
    return ( (bounds[0] + bounds[2])/ 2, (bounds[1] + bounds[3])/ 2);

def most_recent_adt(coll):
	res = coll.find()
	res.skip(res.count() - 1)
	doc = res.next()
	return doc['_acquisitionDate']

def nudge_datetime(dt):
	return dt + datetime.timedelta(microseconds=1000)

def next_timelimits(current):
	since = nudge_datetime(current)
	until = since + datetime.timedelta(days=7)
	return [since, until]

def dt_to_ts(dt, fmt='%Y-%m-%dT%H:%M:%S.%f'):
	s = dt.strftime(fmt)[:-3] + "Z"
	return s

tiles = get_idaho_tiles(results, zoom=12, gbdx=gbdx)


class IdahoScraper(object):
	def __init__(self, start_time, creds, mongo_host='192.168.99.100', mongo_port=27017, 
					search_filters=["(sensorPlatformName = 'WV02' OR sensorPlatformName ='WV03')"], 
					database='idaho', collection='tiles', polling_interval=60, zoom=12):
		self.start_time = start_time

		self._db_name = database
		self._coll_name = collection
		self.mongo = MongoClient(":".join([mongo_host, str(mongo_port)]))
		self.db = self.mongo[self._db_name]
		self.coll = self.db[self._coll_name]
		self.gbdx = Interface(**creds)
		self.polling_interval = polling_interval
		self.filters = search_filters
		self.zoom = zoom

	@property
	def start(self):
		return dt_to_ts(self.since)

	@property
	def end(self):
		return dt_to_ts(self.until)

	def setup(self):
		if self.coll.count() > 0:
			self.since, self.until = next_timelimits(most_recent_adt(self.coll))
		else:
			self.since = self.start_time
			self.until = self.since + datetime.timedelta(days=7)

	def filter_overlap(self, results):
		for img in 


	def idaho_search(self):
    	res  = self.gbdx.catalog.search(startDate=self.start, endDate=self.end, filters=self.filters, types=['IDAHOImage'])
    	def is_unique(identifier):
    		cur = self.coll.find({"identifier": identifier})
    		cnt = cur.count()
    		cur.close()
    		if cnt > 0:
    			return False
    		return True
    	filtered = [r for r in res if self.coll.find({})]

	def collect_tiles(self, img):
	    bounds = loads(img['properties']['footprintWkt']).bounds
	    img_tiles = []
	    tiles = mercantile.tiles(bounds[0], bounds[1], bounds[2], bounds[3], zooms=[self.zoom])
	    for i,t in enumerate(tiles):
	        bnds = mercantile.bounds(t)
	        bbox = (bnds.west, bnds.south, bnds.east, bnds.north)
	        obj = {
	        	"_acquisitionDate": parser.parse(img['properties']['acquisitionDate'])
	            "type": "Feature",
	            "identifier": img['identifier'],
	            "properties": {
	                "value": 100,
	                "idahoID": id,
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

	def poll(self):
		while True:
			results = self.idaho_search(self.since, self.until)
			if len(results) > 0:
				tiles = get_idaho_tiles(results, zoom)
				self.coll.insert_many(tiles)
			else:
				sleep(self.polling_interval)

	def safe_insert(self):



if __name__ == '__main__':
	with open('/src/.creds.json', 'r') as f:
		creds = json.load(f)
	scraper = IdahoScraper()
	scraper.poll()
