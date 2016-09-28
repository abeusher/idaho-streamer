import datetime as dt
import json
from klein import Klein
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from idaho_streamer.util import sleep
from idaho_streamer.error import BadRequest, NotAcceptable, NotFound
from idaho_streamer.db import fake_data

app = Klein()
MAX_POST_BODY = 1024*1024 # 1MB

def parse_json_body(content):
    log.msg("CONTENT: {}".format(content))
    try:
        assert len(content) < MAX_POST_BODY
        result = json.loads(content)
    except AssertionError:
        raise NotAcceptable("Content length: {} too large".format(len(content)))
    except ValueError:
        raise BadRequest("Malformed JSON in request body")
    return result


@app.handle_errors(BadRequest)
def not_acceptable(request, failure):
    request.setResponseCode(400)
    return failure.getErrorMessage()


@app.handle_errors(NotFound)
def not_acceptable(request, failure):
    request.setResponseCode(404)
    return failure.getErrorMessage()


@app.handle_errors(NotAcceptable)
def not_acceptable(request, failure):
    request.setResponseCode(406)
    return failure.getErrorMessage()


@app.route("/filter", methods=["POST"])
def filter_post(request):
    params = parse_json_body(request.content.read())
    fromDate = params.get("fromDate", (dt.datetime.now() - dt.timedelta(weeks=1)).isoformat())
    toDate = params.get("toDate", dt.datetime.now().isoformat())
    bbox = params.get("bbox")
    delay = params.get("delay", 0.1)
    return stream_data(request, fromDate, toDate, bbox, delay)

@inlineCallbacks
def stream_data(request, fromDate, toDate, bbox, delay):
    for doc in fake_data:
        request.write(json.dumps(doc))
        yield sleep(delay)
