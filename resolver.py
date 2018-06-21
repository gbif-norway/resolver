#!/usr/bin/python
# coding: utf-8

import os
import sys
import web
import time
import gdbm
import json
import yaml
import logging
import urllib
import unicodecsv
import cStringIO as StringIO

from rdflib import Graph, plugin
from rdflib.serializer import Serializer
from collections import OrderedDict as Ordered, defaultdict
from web import form, template

config = yaml.load(open("config.yaml"))
db = gdbm.open(config['db'], 'ru')

def group(record):
    d = defaultdict(Ordered)
    for group, terms in config['terms'].iteritems():
        for term in terms:
            if term in record:
                d[group][term] = record.pop(term)
    for k,v in record.iteritems():
        d['other'][k] = v
    return Ordered(sorted(d.items(), key=lambda t: config['order'].index(t[0])))

def clean(record):
    clean = {}
    for k,v in record.iteritems():
        if k.startswith("_"): continue
        elif v == "": continue
        elif k == "id": continue
        else: clean[k] = v
    return clean

def prefix(record):
    prefixed = {}
    for k,v in record.iteritems():
        if k.find(":") > 0: prefixed[k] = v
        else: prefixed["dwc:%s" % k] = v
    return prefixed

def graph(k, r, p, g):
    return Graph().parse(data=jsonld(k, r, p, g), format='json-ld')

def html(key, record, prefixed, grouped):
    return render.record(key, record, prefixed, grouped)

def csv(key, record, prefixed, grouped):
    web.header('Content-Type', 'text/csv; charset=utf-8')
    buf = StringIO.StringIO()
    writer = unicodecsv.DictWriter(buf, record.keys())
    writer.writeheader()
    writer.writerow(record)
    buf.seek(0)
    yield(buf.read())

def text(key, record, prefixed, grouped):
    web.header('Content-Type', 'text/plain; charset=utf-8')
    for k,v in record.iteritems():
        yield("%s\t%s\n" % (k, v))

def n3(key, record, prefixed, grouped):
    web.header('Content-Type', 'text/n3; charset=utf-8')
    return graph(key, record, prefixed, grouped).serialize(format='n3')

def rdf(key, record, prefixed, grouped):
    web.header('Content-Type', 'application/rdf+xml; charset=utf-8')
    return graph(key, record, prefixed, grouped).serialize()

def jsonld(key, record, prefixed, grouped):
    prefixed['@id'] = "http://purl.org/gbifnorway/id/%s" % key
    prefixed['@context'] = {
        "dc": "http://purl.org/dc/elements/1.1/", 
        "dwc": "http://rs.tdwg.org/dwc/terms/"
    }
    return json.dumps(prefixed, sort_keys=True, indent=2)

mimes = {
    '*/*': html,
    'text/html': html,
    'application/json': jsonld,
    'application.ld+json': jsonld,
    'text/plain': text,
    'text/n3': n3,
    'text/turtle': n3,
    'application/rdf+xml': rdf
}

exts = {
    '.html': html,
    '.csv': csv,
    '.txt': text,
    '.n3': n3,
    '.rdf': rdf,
    '.json': jsonld
}

def strfepoch(epoch, fmt="%Y-%m-%d"):
    return time.strftime(fmt, time.localtime(float(epoch)))

helper = { 'time': strfepoch, 'config': config }
render = template.render('templates', globals=helper)

class index:
    def GET(self):
        q = web.input()
        if 'id' in q:
            raise web.seeother("/resolver/%s" % q['id'])
        return render.index(len(db))

class resolver:
    def GET(self, raw):
        key, ext = os.path.splitext(raw)
        key = key.replace("urn:uuid:", "")
        key = key.replace("urn:catalog:", "")
        mime = web.ctx.env.get('CONTENT_TYPE')
        if "HTTP_ACCEPT" in web.ctx.env:
          accept = web.ctx.env.get("HTTP_ACCEPT", "text/html")
          for part in accept.split(","):
            m = part.split(";")[0]
            if m in mimes:
              mime = m
              break
        if str(key) in db:
            record = json.loads(db[key])
            cleaned = clean(record)
            if 'id' in cleaned: del cleaned['id']
            prefixed = prefix(cleaned)
            grouped = group(cleaned.copy())
            prefixed['dc:title'] = cleaned.get('scientificName')
            if ext: viewer = exts[ext] or html
            elif mime: viewer = mimes[mime] or html
            else: viewer = html
            return viewer(key, cleaned, prefixed, grouped)
        else:
            return web.notfound()

urls = (
    '/resolver', 'index',
    '/resolver/', 'index',
    '/resolver/(.+)', 'resolver',
)

app = web.application(urls, locals())

if __name__ == "__main__":
    app.run()

