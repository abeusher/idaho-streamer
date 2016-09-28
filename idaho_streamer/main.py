import datetime as dt
import json
from bson.json_util import dumps as json_dumps
from klein import Klein
from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from shapely.geometry import shape, Polygon
from dateutil.parser import parse as parse_date

from idaho_streamer.util import sleep
from idaho_streamer.error import BadRequest, NotAcceptable, NotFound
from idaho_streamer.db import db

app = Klein()
MAX_POST_BODY = 1024*1024 # 1MB

def parse_json_body(content):
    try:
        assert len(content) < MAX_POST_BODY
        result = json.loads(content)
    except AssertionError:
        raise NotAcceptable("Content length: {} too large".format(len(content)))
    except ValueError:
        raise BadRequest("Malformed JSON in request body")
    return result

def parse_bbox(bbox):
    bbox = bbox.split(",")
    try:
        assert len(bbox) == 4
        bbox = [float(v) for v in bbox]
        bbox = Polygon([[bbox[0], bbox[1]],
                        [bbox[0], bbox[3]],
                        [bbox[2], bbox[3]],
                        [bbox[2], bbox[1]],
                        [bbox[0], bbox[1]]])
    except AssertionError, ValueError:
        raise BadRequest("Expected 4 comma separated numbers.  Got {}".format(bbox))
    return bbox

@app.handle_errors(BadRequest)
def bad_request(request, failure):
    request.setResponseCode(400)
    return failure.getErrorMessage()


@app.handle_errors(NotFound)
def not_found(request, failure):
    request.setResponseCode(404)
    return failure.getErrorMessage()


@app.handle_errors(NotAcceptable)
def not_acceptable(request, failure):
    request.setResponseCode(406)
    return failure.getErrorMessage()


@app.route("/filter", methods=["POST"])
def filter_post(request):
    params = parse_json_body(request.content.read())
    fromDate = parse_date(params.get("fromDate", (dt.datetime.now() - dt.timedelta(weeks=1)).isoformat()))
    toDate = parse_date(params.get("toDate", dt.datetime.now().isoformat()))
    bbox = params.get("bbox")
    if bbox is not None:
        bbox = parse_bbox(bbox)
    try:
        delay = float(params.get("delay", "0.0"))
    except ValueError:
        raise BadRequest("Delay should be a floating point number")
    request.setHeader('Content-Type', 'application/json')
    request.setResponseCode(200)
    return stream_data(request, fromDate, toDate, bbox, delay)


@inlineCallbacks
def stream_data(request, fromDate, toDate, bbox, delay):
    docs, d  = yield db.idaho_tiles.find({"_acquisitionDate": {"$gte": fromDate, "$lt": toDate}},
                                         fields={"_id": False, "_acquisitionDate": False}, cursor=True)
    while docs:
        for doc in docs:
            if bbox is None or bbox.intersects(shape(doc["geometry"])):
                request.write(json_dumps(doc))
                yield sleep(delay)
        docs, d = yield d
