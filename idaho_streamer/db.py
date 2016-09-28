import os
from twisted.python import log
from twisted.internet import ssl
from twisted.internet.defer import inlineCallbacks, returnValue
from txmongo.connection import ConnectionPool

COLLECTIONS = ["idaho_tiles"]

use_ssl=False
assert "MONGO_CONNECTION_STRING" in os.environ
mongo_url = os.environ.get("MONGO_CONNECTION_STRING")


#put something in a database

from itertools import cycle

fake_post = {"fromDate": "Tue Aug 06 02:23:21 +0000 2013", "toDate": ""}
d1 = {"zxy": [12, 2632, 1616], "url": "http://bullshit.com", "idahoID": "cd1adcb2-84ca-45da-8185-5a3bb8e34b2b",
"bounds": [51.328125, 35.389, 51.416, 35.460], "center": [51.37, 35.424], "created_at": "Tue Aug 06 02:23:21 +0000 2013"}
d2 = d1.copy()
d2['created_at'] = "Wed Aug 07 02:23:21 +0000 2013"
d3 = d1.copy()
d3['created_at'] = "Fri Aug 09 02:23:21 +0000 2013"
fake_data = [d1, d2, d3]
fake_cycle = cycle(fake_data)



@inlineCallbacks
def connect():
    if use_ssl:
        connection = yield ConnectionPool(mongo_url, ssl_context_factory=ssl.ClientContextFactory())
    else:
        connection = yield ConnectionPool(mongo_url)
    returnValue(connection.get_default_database())


@inlineCallbacks
def create_collections(db):
    current_collections = yield db.collection_names()
    created = []
    for key in COLLECTIONS:
        if key not in current_collections:
            yield db.create_collection(key)
            created.append(key)
    log.msg("Created collections: {}".format(",".join(created)))
    returnValue(created)

@inlineCallbacks
def drop_collections(db):
    dropped = []
    for key in COLLECTIONS:
        try:
            yield db.drop_collection(key)
            dropped.append(key)
        except:
            pass
    log.msg("Dropped collections: {}".format(",".join(dropped)))
    returnValue(dropped)

@inlineCallbacks
def populate(db):
    yield db.idaho_tiles.insert_many(fake_data)

@inlineCallbacks
def init():
    db = yield connect()
    yield drop_collections(db)
    yield create_collections(db)
    yield populate(db)
    c = yield db.idaho_tiles.count()
    log.msg("Database Ready. {} records present".format(c))
    returnValue(db)

get_db = init()
