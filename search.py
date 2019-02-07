#!/usr/bin/python
# coding: utf-8

import os
import sys
import json
import yaml
import time
import urllib
import unicodecsv
import web
import cStringIO as StringIO

# Test comment
from elasticsearch import Elasticsearch
from web import form, template

config = yaml.load(open("search.yaml"))
es = Elasticsearch(config['hosts'], timeout=config['timeout'])

def strfepoch(epoch, fmt="%Y-%m-%d"):
    return time.strftime(fmt, time.localtime(float(epoch)))

helper = { 'time': strfepoch, 'config': config }
render = template.render('templates', globals=helper)

class index:
    def GET(self):
        q = web.input().get("q", "")
        results = es.search(index=config['index'], q=q).get("hits", {})
        if q:
            return render.search(q, results)
        return render.search(q, None)

urls = (
    '/search', 'index',
)

app = web.application(urls, locals())

if __name__ == "__main__":
    app.run()

