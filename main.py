import txmongo
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, succeed
from twisted.python import log
from klein import run, route, Klein
import simplejson as json

#put something in a database

class IdahoStreamer(object):
    app = Klein()

    def __init__(self, name='jamie'):
        self.name = name

    @app.route('/')
    def hello(self, request):
        return "Hello {}".format(name)

    @app.route('/filter', methods=['POST'])
    def push_data(self, request):
        global name
        #request.setHeader('Content-Type', 'application/json')
        name = request.args.get('data', ["world"])[0]
        return name

    @app.route('/stream/<string:q>', methods=['GET'])
    def get_query_params(self, request, q):
        return q

    @inlineCallbacks
    def put_something_in_db(self, data):
        mongo = yield txmongo.MongoConnection()
        foo = mongo.foo
        test = foo.test
        result = yield test.insert({"something": data})
        returnValue(result)


if __name__ == '__main__':
    streamer = IdahoStreamer()
    streamer.app.run('localhost', 8080)
    #streamer.put_something_in_db(5)



# @route('/')
# def home(request):
#   return "Hello, world!"

# run("localhost", 8080)