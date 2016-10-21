import os.path
import datetime as dt
import json
from bson.json_util import dumps as json_dumps
from klein import Klein
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread
from twisted.internet.task import LoopingCall
from twisted.web.static import File
from twisted.python import log
from txmongo import filter as qf
from shapely.geometry import shape, Polygon
from dateutil.parser import parse as parse_date
import treq

from idaho_streamer.util import sleep
from idaho_streamer.error import BadRequest, NotAcceptable, NotFound, Unauthorized
from idaho_streamer.db import db
from idaho_streamer.aws import vrt_for_id, invoke_lambda

from lambdify import deploy as create_lambda

app = Klein()
MAX_POST_BODY = 1024*1024 # 1MB
POLL_INTERVAL = 15.0 # seconds


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

@app.handle_errors(Unauthorized)
def unauthorized(request, failure):
    request.setResponseCode(401)
    return failure.getErrorMessage()


@app.route("/public/", branch=True)
def index(request):
    # TODO: disable directory listing
    return File(os.path.join("./idaho-streamer", "idaho_streamer", "public"))


@app.route("/lambdify", methods=["POST"])
@inlineCallbacks
def lambdifier(request):
    params = parse_json_body(request.content.read())
    # TODO: Validate a GBDX Token
    auth_headers = request.requestHeaders.getRawHeaders("Authorization")
    if auth_headers is None or len(auth_headers) == 0:
        raise Unauthorized("A valid GBDX token is required.")
    else:
        auth_header = auth_headers[0]
    resp = yield treq.get("https://geobigdata.io/workflows/v1/authtest",
                          headers={"Authorization": auth_header})
    if resp.code != 200:
        raise Unauthorized("A valid GBDX token is required.")
    yield deferToThread(create_lambda, params["name"], fn=params["code"])

@app.route("/<string:idaho_id>.json")
@inlineCallbacks
def footprint(request, idaho_id="unknown"):
    rec = yield db.idaho_footprints.find_one({"id": idaho_id})
    if rec is None:
        raise NotFound
    else:
        request.setHeader('Content-Type', 'application/json')
        request.setResponseCode(200)
        returnValue(json_dumps(rec))


@app.route("/<string:idaho_id>/<string:node>/<int:level>.vrt")
@inlineCallbacks
def toa_vrt(request, idaho_id="unknown", node="TOAReflectance", level=0):
    rec = yield db.idaho_footprints.find_one({"id": idaho_id})
    if rec is None:
        raise NotFound
    request.setHeader("Content-Type","application/xml")
    request.setResponseCode(200)
    try:
        vrt = yield deferToThread(vrt_for_id, idaho_id, rec["properties"], level, node)
    except IndexError:
        raise NotFound
    returnValue(vrt)

@app.route("/<string:idaho_id>/<string:lambda_name>/<int:z>/<int:x>/<int:y>")
@inlineCallbacks
def tms(request, idaho_id="unknown", lambda_name="default", z=0, x=0, y=0):
    tile_url = yield deferToThread(invoke_lambda, lambda_name, idaho_id, z, x, y)
    request.redirect(tile_url)

@app.route("/filter", methods=["POST"])
@inlineCallbacks
def filter_post(request):
    params = parse_json_body(request.content.read())
    fromDate = parse_date(params.get("fromDate", (dt.datetime.now() - dt.timedelta(weeks=1)).isoformat()))
    toDate = params.get("toDate")
    enable_streaming = False
    if toDate is None:
        enable_streaming = True
        toDate = dt.datetime.now().isoformat()
    toDate = parse_date(toDate)
    bbox = params.get("bbox")
    if bbox is not None:
        bbox = parse_bbox(bbox)
    try:
        delay = float(params.get("delay", "0.0"))
    except ValueError:
        raise BadRequest("Delay should be a floating point number")
    minCloudCover = params.get("minCloudCover", 0.0)
    maxCloudCover = params.get("maxCloudCover", float('inf'))

    request.setHeader('Content-Type', 'application/json')
    request.setResponseCode(200)
    last_id = yield backfill(request, fromDate, toDate, minCloudCover, maxCloudCover, bbox, delay)
    if enable_streaming:
        yield stream(request, last_id, minCloudCover, maxCloudCover, bbox, delay)


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


@inlineCallbacks
def backfill(request, fromDate, toDate, minCloudCover, maxCloudCover, bbox, delay):
    last_rec = yield db.idaho_footprints.find({}, fields={"_id": True}, limit=1, filter=qf.sort(qf.DESCENDING("_id")))
    last_id = last_rec[0]["_id"]
    docs, d  = yield db.idaho_footprints.find({"_acquisitionDate": {"$gte": fromDate, "$lt": toDate}},
                                         fields={"_acquisitionDate": False}, cursor=True,
                                         filter=qf.sort(qf.ASCENDING("_acquisitionDate")))
    while docs:
        for doc in docs:
            last_id = doc["_id"]
            if bbox is None or bbox.intersects(shape(doc["geometry"])):
                request.write(json_dumps(doc))
                request.write("\n")
                yield sleep(delay)
        docs, d = yield d
    returnValue(last_id)


@inlineCallbacks
def stream(request, from_id, minCloudCover, maxCloudCover, bbox, delay):
    while True:
        ref = dt.datetime.now()
        docs = yield db.idaho_footprints.find({"_id": {"$gt": from_id, "$gte": minCloudCover, "$lt": maxCloudCover}},
                                              filter=qf.sort(qf.ASCENDING("_acquisitionDate")))
        for doc in docs:
            if bbox is None or bbox.intersects(shape(doc["geometry"])):
                request.write(json_dumps(doc))
                request.write("\n")
                yield sleep(delay)
        delta = (dt.datetime.now() - ref).total_seconds()
        yield sleep(max(0.0, POLL_INTERVAL - delta))
