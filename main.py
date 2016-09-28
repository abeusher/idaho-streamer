from __future__ import print_function
import txmongo
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, succeed, Deferred
from twisted.python import log
from klein import run, route, Klein
import simplejson as json
import datetime as dt
from dateutil import parser
from twisted.internet.protocol import Protocol


#put something in a database

from itertools import cycle

fake_post = {"fromDate": "Tue Aug 06 02:23:21 +0000 2013", "toDate": ""}

d1 = {"zxy": [12, 2632, 1616], "url": "http://bullshit.com", "idahoID": "cd1adcb2-84ca-45da-8185-5a3bb8e34b2b",
"bounds": [51.328125, 35.389, 51.416, 35.460], "center": [51.37, 35.424], "created_at": "Tue Aug 06 02:23:21 +0000 2013"}

d2 = d1.copy()
d2['created_at'] = "Wed Aug 07 02:23:21 +0000 2013"
d3 = d1.copy()
d3['created_at'] = "Fri Aug 09 02:23:21 +0000 2013"

def sleep(delay, reactor=None):
    if not reactor:
        from twisted.internet import reactor
    d = Deferred()
    reactor.callLater(delay, d.callback, None)
    return d

fake_data = cycle([d1, d2, d3])

class IdahoStreamer(object):
    app = Klein()

    def __init__(self, name='jamie'):
        self.name = name
        self.data = fake_data
 
        self.setup()

    def timestamp_to_datetime(self, ts):
        return parser.parse(ts)

    @inlineCallbacks
    def setup(self):
        self.mongo = yield txmongo.MongoConnection('192.168.99.100', 27017)
        try:
            self.foo = self.mongo.foo
        except Exception as e:
            log.msg(str(e))
        self.test = self.foo.test

        @inlineCallbacks
        def insert():
            for i in range(3):
                d = self.data.next()
                d['dt_created_at'] = self.timestamp_to_datetime(d['created_at'])
                log.msg(str(d))
                yield self.test.insert_one(d, safe=True)
            returnValue(None)
        try:
            yield insert()
        except Exception as e:
            log.msg(str(e))

    # @app.route('/filter', methods=['POST'])
    # @inlineCallbacks
    # def push_data(self, request):
    #     request.setHeader('Content-Type', 'application/json')
    #     body = json.load(request.content)
    #     _to = body.get('toTime', dt.datetime.now())
    #     try:
    #         _from = body['fromTime']
    #     except KeyError as ke:
    #         request.setResponseCode(404)
    #     while True:
    #         r = yield self.data.next()
    #         log.msg(json.dumps(r))
    #         request.write(str(r))
    #         yield sleep(2)
    @app.route('/filter', methods=['POST'])
    @inlineCallbacks
    def push_data(self, request):
        request.setHeader('Content-Type', 'application/json')
        body = json.load(request.content)
        log.msg(str(body))
        _to = body.get('toTime') 
        if _to is not None:
            _to = self.timestamp_to_datetime(_to)
        else:
            _to = dt.datetime.utcnow()
        try:
            _from = body['fromTime']
        except KeyError as ke:
            request.setResponseCode(404)
        # cursor = yield self.test.find({"created_at": {"$gte": _from}}, cursor_type=CursorType.AWAIT_TAILABLE)
        _from = self.timestamp_to_datetime(_from)
        q = {"dt_created_at": {"$gte": _from}}
        log.msg(str(q))
        # @inlineCallbacks
        # def query(q):
        #     docs, dfr = yield self.test.find(q, cursor=True)
        #     while docs:
        #         for doc in docs:
        #             log.msg(json.dumps(doc))
        #             request.write(str(doc))
        #         docs, dfr = yield dfr
        #     returnValue(docs)


        # yield query(q)

        #request.finish()
        cursor = yield self.test.find(q)
        for doc in cursor:
            request.write(str(doc))
            yield sleep(2)
            log.msg("writitng doc")

    @app.route('/stream/<string:q>', methods=['GET'])
    def get_query_params(self, request, q):
        return q


if __name__ == '__main__':
    import sys
    log.startLogging(sys.stdout)
    streamer = IdahoStreamer()

    streamer.app.run('localhost', 8080)

    # streamer.put_something_in_db(5)



# @route('/')
# def home(request):
#   return "Hello, world!"

# run("localhost", 8080)