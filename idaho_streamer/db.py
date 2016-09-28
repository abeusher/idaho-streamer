import txmongo

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
