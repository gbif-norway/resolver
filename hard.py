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
db.open("dwc.db", cabinet.TDBOWRITER)
rec = db.get("O:L:14")
rec['occurrenceID'] = "urn:uuid:41d9cbb4-4590-4265-8079-ca44d46d27c3"
rec['id'] = "urn:uuid:41d9cbb4-4590-4265-8079-ca44d46d27c3"

db.put("O:L:14", rec)
db.put("41d9cbb4-4590-4265-8079-ca44d46d27c3", rec)

