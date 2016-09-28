import os
import os.path
from twisted.python import log
from twisted.internet import ssl
from twisted.internet.defer import inlineCallbacks, returnValue
from txmongo.connection import ConnectionPool
from txmongo import filter as qf

COLLECTIONS = ["idaho_tiles"]

use_ssl=False
assert "MONGO_CONNECTION_STRING" in os.environ
mongo_url = os.environ.get("MONGO_CONNECTION_STRING")


from bson.json_util import loads
fixture_file = os.path.join(os.path.dirname(__file__), "..", "test", "fixtures", "tile_dump.json")

with open(fixture_file) as f:
    fixture_data = loads(f.read())

if use_ssl:
    connection = ConnectionPool(mongo_url, ssl_context_factory=ssl.ClientContextFactory())
else:
    connection = ConnectionPool(mongo_url)
db = connection.get_default_database()


@inlineCallbacks
def create_collections(db):
    current_collections = yield db.collection_names()
    created = []
    for key in COLLECTIONS:
        if key not in current_collections:
            yield db.create_collection(key)
            created.append(key)
    # idaho_tiles index
    db.idaho_tiles.create_index(qf.sort(qf.ASCENDING("_acquisitionDate")))
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
    yield db.idaho_tiles.insert_many(fixture_data)


@inlineCallbacks
def init():
    yield drop_collections(db)
    yield create_collections(db)
    yield populate(db)
    c = yield db.idaho_tiles.count()
    log.msg("Database Ready. {} records present".format(c))
    returnValue(connection)

init()
