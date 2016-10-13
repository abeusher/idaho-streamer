# IDAHO Streamer

REST streaming API for getting updates about available IDAHO imagery.

## Setup

This repo uses Git LFS to store a data dump for testing.  Before cloning, setup Git LFS on your system via the instructions [here](https://git-lfs.github.com/).  After that `git clone` will also download the data dump.

In order to use this repository, DigitalGlobe AWS credentials must be setup in a profile called `dg`.  Instructions for setting up awscli profiles can be found [in the AWS docs](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-multiple-profiles)

## Installation (for development)

```
$ pip install -r requirements.txt
$ python setup.py develop
$ export MONGO_CONNECTION_STRING="mongodb://<your_mongo_instance>:<port>/<database>"
$ ./start.sh
```

Currently, starting the server will drop and recreate the collections and fill them with fixture data from the data dump.  This can take a little bit of time.

## Usage (for testing)

```
$ curl -X POST -d '{"fromDate": "date string", "toDate": "date string", "bbox": "minx,miny,maxx,maxy", "delay": 0.1}' http://localhost:8080/filter
```

All parameters are optional, however a json object must be sent in the post body.  By default, the stream will return all tiles from the past week with no delay.  If toDate is set, the stream will terminate.
