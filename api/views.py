from django.http import HttpResponse
from elasticsearch import Elasticsearch
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import json


@csrf_exempt
def index(request, name):
    if request.method != 'GET' and request.method != 'POST':
        return HttpResponse("This method is not allowed!\n")
    if name != 'file' and name != 'organism' and name != 'specimen' and name != 'dataset' and name != 'experiment':
        return HttpResponse("This method is not allowed!\n")
    size = request.GET.get('size', 10)
    es = Elasticsearch([settings.NODE1, settings.NODE2])
    if request.body:
        results = es.search(index=name, size=size, body=json.loads(request.body.decode("utf-8")))
    else:
        field = request.GET.get('_source', '')
        sort = request.GET.get('sort', '')
        query = request.GET.get('q', '')
        if query != '':
            results = es.search(index=name, size=size, _source=field, sort=sort, q=query)
        else:
            results = es.search(index=name, size=size, _source=field, sort=sort)
    results = json.dumps(results)
    return HttpResponse(results)


@csrf_exempt
def detail(request, name, id):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
    es = Elasticsearch([settings.NODE1, settings.NODE2])
    results = es.search(index=name, q="_id:{}".format(id))
    if results['hits']['total'] == 0:
        results = es.search(index=name, q="alternativeId:{}".format(id))
    results = json.dumps(results)
    return HttpResponse(results)


@csrf_exempt
def get_samples_protocols(request):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
    es = Elasticsearch([settings.NODE1, settings.NODE2])
    results = es.search(index="specimen", size=100000)
    entries = {}
    for result in results["hits"]["hits"]:
        if "specimenFromOrganism" in result["_source"]:
            key = result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['filename']
            try:
                protocol_type = result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['url'].split("/")[5]
            except:
                protocol_type = ""
            parsed = key.split("_")
            if parsed[0] in settings.UNIVERSITIES:
                name = settings.UNIVERSITIES[parsed[0]]
                protocol_name = " ".join(parsed[2:-1])
                date = parsed[-1].split(".")[0]
                entries.setdefault(key, {"name": "", "date": "", "protocol_name": "", "key": "", "protocol_type": ""})
                entries[key]['university_name'] = name
                entries[key]['protocol_date'] = date[0:4]
                entries[key]["protocol_name"] = protocol_name
                entries[key]["key"] = key
                if protocol_type in ["analysis", "assays", "samples"]:
                    entries[key]["protocol_type"] = protocol_type
    results = json.dumps(list(entries.values()))
    return HttpResponse(results)


@csrf_exempt
def get_samples_protocol_details(request, id):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
    es = Elasticsearch([settings.NODE1, settings.NODE2])
    results = es.search(index="specimen", size=100000)
    entries = {}
    for result in results["hits"]["hits"]:
        if "specimenFromOrganism" in result["_source"] and \
                id == result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['filename']:
            key = id
            url = result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['url']
            parsed = key.split("_")
            if parsed[0] in settings.UNIVERSITIES:
                name = settings.UNIVERSITIES[parsed[0]]
                protocol_name = " ".join(parsed[2:-1])
                date = parsed[-1].split(".")[0]
                entries.setdefault(key, {"specimen": [], "university_name": "", "protocol_date": "",
                                         "protocol_name": "", "key": "", "url": ""})
                specimen = {"id": "", "organism_part_cell_type": "", "organism": "", "breed": "", "derived_from": ""}
                specimen["id"] = result["_id"]
                specimen["organism_part_cell_type"] = result["_source"]["cellType"]["text"]
                specimen["organism"] = result["_source"]["organism"]["organism"]["text"]
                specimen["breed"] = result["_source"]["organism"]["breed"]["text"]
                specimen["derived_from"] = result["_source"]["derivedFrom"]
                entries[key]["specimen"].append(specimen)
                entries[key]['university_name'] = name
                entries[key]['protocol_date'] = date
                entries[key]["protocol_name"] = protocol_name
                entries[key]["key"] = key
                entries[key]["url"] = url
    results = json.dumps(list(entries.values()))
    return HttpResponse(results)


@csrf_exempt
def get_files_protocols(request):
    return_results = {}
    es = Elasticsearch([settings.NODE1, settings.NODE2])
    results = es.search(index="experiment", size=100000)

    def expand_object(data, assay='', target=''):
        for key in data:
            if isinstance(data[key], dict):
                if 'filename' in data[key]:
                    if data[key]['filename'] != '':
                        if assay == '' and target == '':
                            data_key = "{}-{}-{}".format(key, data['assayType'], data['experimentTarget'])
                            return_results.setdefault(data_key, {'name': key,
                                                                 'experimentTarget': data['experimentTarget'],
                                                                 'assayType': data['assayType'],
                                                                 'key': data_key})
                        else:
                            data_key = "{}-{}-{}".format(key, assay, target)
                            return_results.setdefault(data_key, {'name': key,
                                                                 'experimentTarget': target,
                                                                 'assayType': assay,
                                                                 'key': data_key})
                else:
                    expand_object(data[key], data['assayType'], data['experimentTarget'])

    for item in results['hits']['hits']:
        expand_object(item['_source'])
    results = json.dumps(list(return_results.values()))
    return HttpResponse(results)


@csrf_exempt
def get_files_protocol_details(request, id):
    return_results = {}
    es = Elasticsearch([settings.NODE1, settings.NODE2])
    results = es.search(index="experiment", size=100000)

    def expand_object(data, assay='', target='', accession='', storage='', processing=''):
        for key in data:
            if isinstance(data[key], dict):
                if 'filename' in data[key]:
                    if data[key]['filename'] != '':
                        if assay == '' and target == '' and accession == '' and storage == '' and processing == '':
                            data_key = "{}-{}-{}".format(key, data['assayType'], data['experimentTarget'])
                            data_experiment = "{}|{}|{}".format(data['accession'], data['sampleStorage'],
                                                                data['sampleStorageProcessing'])
                            if data_key == id:
                                return_results.setdefault(data_key, {'name': key,
                                                                     'experimentTarget': data['experimentTarget'],
                                                                     'assayType': data['assayType'],
                                                                     'key': data_key,
                                                                     'url': data[key]['url'],
                                                                     'filename': data[key]['filename'],
                                                                     'experiments': []})
                                return_results[data_key]['experiments'].append(data_experiment)
                        else:
                            data_key = "{}-{}-{}".format(key, assay, target)
                            data_experiment = "{}|{}|{}".format(accession, storage, processing)
                            if data_key == id:
                                return_results.setdefault(data_key, {'name': key,
                                                                     'experimentTarget': target,
                                                                     'assayType': assay,
                                                                     'key': data_key,
                                                                     'url': data[key]['url'],
                                                                     'filename': data[key]['filename'],
                                                                     'experiments': []})
                                return_results[data_key]['experiments'].append(data_experiment)
                else:
                    expand_object(data[key], data['assayType'], data['experimentTarget'], data['accession'],
                                  data['sampleStorage'], data['sampleStorageProcessing'])

    for item in results['hits']['hits']:
        expand_object(item['_source'])
    results = json.dumps(list(return_results.values()))
    return HttpResponse(results)
