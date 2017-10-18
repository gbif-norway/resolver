#!/usr/bin/python
# coding: utf-8

import os
import sys
import web
import time
import json
import yaml
import logging
import urllib
import unicodecsv

import cStringIO as StringIO
from rdflib import Graph, plugin
from rdflib.serializer import Serializer

from collections import OrderedDict, defaultdict

from web import form, template
from tokyo import cabinet

db = cabinet.TDB()
db.open("dwc.db", cabinet.TDBOREADER)

for k in db:
  rec = db.get(k)
  if 'measurementID' in rec:
    print(k)

