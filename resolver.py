#!/usr/bin/python
# coding: utf-8

import os
import sys
import web
import json
import yaml
import logging
import urllib
import unicodecsv

import cStringIO as StringIO
from rdflib import Graph, plugin
from rdflib.serializer import Serializer

# from yapsy.PluginManager import PluginManager

from collections import OrderedDict, defaultdict

from web import form, template
from tokyo import cabinet

dwc = {
    'record': ['dcterms:type', 'dcterms:modified', 'dcterms:language', 'dcterms:license', 'dcterms:rightsHolder', 'dcterms:accessRights', 'dcterms:bibliographicCitation', 'dcterms:references, institutionID', 'collectionID', 'datasetID', 'institutionCode', 'collectionCode', 'datasetName', 'ownerInstitutionCode', 'basisOfRecord', 'informationWithheld', 'dataGeneralizations', 'dynamicProperties'],

    'occurrence': ['occurrenceID', 'catalogNumber', 'recordNumber', 'recordedBy', 'individualCount', 'organismQuantity', 'organismQuantityType', 'sex', 'lifeStage', 'reproductiveCondition', 'behavior', 'establishmentMeans', 'occurrenceStatus', 'preparations', 'disposition', 'associatedMedia', 'associatedReferences', 'associatedSequences', 'associatedTaxa', 'otherCatalogNumbers', 'occurrenceRemarks'],

    'organism': ['organismID', 'organismName', 'organismScope', 'associatedOccurrences', 'associatedOrganisms', 'previousIdentifications', 'organismRemarks'],

    'material': ['materialSampleID'],

    'event': ['eventID', 'parentEventID', 'fieldNumber', 'eventDate', 'eventTime', 'startDayOfYear', 'endDayOfYear', 'year', 'month', 'day', 'verbatimEventDate', 'habitat', 'samplingProtocol', 'sampleSizeValue', 'sampleSizeUnit', 'samplingEffort', 'fieldNotes', 'eventRemarks'],

    'location': ['locationID', 'higherGeographyID', 'higherGeography', 'continent', 'waterBody', 'islandGroup', 'island', 'country', 'countryCode', 'stateProvince', 'county', 'municipality', 'locality', 'verbatimLocality', 'minimumElevationInMeters', 'maximumElevationInMeters', 'verbatimElevation', 'minimumDepthInMeters', 'maximumDepthInMeters', 'verbatimDepth', 'minimumDistanceAboveSurfaceInMeters', 'maximumDistanceAboveSurfaceInMeters', 'locationAccordingTo', 'locationRemarks', 'decimalLatitude', 'decimalLongitude', 'geodeticDatum', 'coordinateUncertaintyInMeters', 'coordinatePrecision', 'pointRadiusSpatialFit', 'verbatimCoordinates', 'verbatimLatitude', 'verbatimLongitude', 'verbatimCoordinateSystem', 'verbatimSRS', 'footprintWKT', 'footprintSRS', 'footprintSpatialFit', 'georeferencedBy', 'georeferencedDate', 'georeferenceProtocol', 'georeferenceSources', 'georeferenceVerificationStatus', 'georeferenceRemarks'],

    'geology': ['geologicalContextID', 'earliestEonOrLowestEonothem', 'latestEonOrHighestEonothem', 'earliestEraOrLowestErathem', 'latestEraOrHighestErathem', 'earliestPeriodOrLowestSystem', 'latestPeriodOrHighestSystem', 'earliestEpochOrLowestSeries', 'latestEpochOrHighestSeries', 'earliestAgeOrLowestStage', 'latestAgeOrHighestStage', 'lowestBiostratigraphicZone', 'highestBiostratigraphicZone', 'lithostratigraphicTerms', 'group', 'formation', 'member', 'bed'],

    'identification': ['identificationID', 'identificationQualifier', 'typeStatus', 'identifiedBy', 'dateIdentified', 'identificationReferences', 'identificationVerificationStatus', 'identificationRemarks'],

    'taxon': ['taxonID', 'scientificNameID', 'acceptedNameUsageID', 'parentNameUsageID', 'originalNameUsageID', 'nameAccordingToID', 'namePublishedInID', 'taxonConceptID', 'scientificName', 'acceptedNameUsage', 'parentNameUsage', 'originalNameUsage', 'nameAccordingTo', 'namePublishedIn', 'namePublishedInYear', 'higherClassification', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'subgenus', 'specificEpithet', 'infraspecificEpithet', 'taxonRank', 'verbatimTaxonRank', 'scientificNameAuthorship', 'vernacularName', 'nomenclaturalCode', 'taxonomicStatus', 'nomenclaturalStatus', 'taxonRemarks'],

    'measurements': ['measurementID', 'measurementType', 'measurementValue', 'measurementAccuracy', 'measurementUnit', 'measurementDeterminedBy', 'measurementDeterminedDate', 'measurementMethod', 'measurementRemarks'],

    'relationships': ['resourceRelationshipID', 'resourceID', 'relatedResourceID', 'relationshipOfResource', 'relationshipAccordingTo', 'relationshipEstablishedDate', 'relationshipRemarks'],
}

dwcorder = ['record', 'occurrence', 'organism', 'material', 'event', 'location', 'geology', 'identification', 'taxon', 'measurements', 'relationships', 'other']

def group(record):
    d = defaultdict(OrderedDict)
    for group, terms in dwc.iteritems():
        for term in terms:
            if term in record:
                d[group][term] = record.pop(term)
    for k,v in record.iteritems():
        d['other'][k] = v
    return OrderedDict(sorted(d.items(), key=lambda t: dwcorder.index(t[0])))

def prefix(record):
    prefixed = {}
    for k,v in record.iteritems():
        if k.find(":") > 0: prefixed[k] = v
        else: prefixed["dwc:%s" % k] = v
    return prefixed

def resolve(key):
    db = cabinet.TDB()
    db.open('dwc.db', cabinet.TDBOREADER | cabinet.TDBONOLCK)
    try:
        record = db.get(key)
        db.close()
        return record
    except:
        db.close()

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

def graph(k, r, p, g):
    return Graph().parse(data=jsonld(k, r, p, g), format='json-ld')

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
    'text/plain': text,
    'text/n3': n3,
    'text/turtle': n3,
    'application/rdf+xml': rdf,
    'application/json': jsonld,
    'application.ld+json': jsonld
}

exts = {
    '.html': html,
    '.csv': csv,
    '.txt': text,
    '.n3': n3,
    '.rdf': rdf,
    '.json': jsonld
}

render = template.render('templates')

class index:
    def GET(self):
        return render.index(0)

class resolver:
    def GET(self, raw):
        key, ext = os.path.splitext(raw)
        mime = web.ctx.env.get('CONTENT_TYPE')
        record = resolve(key)
        if record:
            prefixed = prefix(record)
            grouped = group(record.copy())
            if ext: viewer = exts[ext] or html
            elif mime: viewer = mimes[mime] or html
            else: viewer = html
            return viewer(key, record, prefixed, grouped)
        else:
            return web.notfound()

urls = (
    '/', 'index',
    '/([^/]+)', 'resolver',
)

app = web.application(urls, locals())

if __name__ == "__main__":
    app.run()

