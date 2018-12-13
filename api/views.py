from django.http import HttpResponse
from elasticsearch import Elasticsearch
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import json


@csrf_exempt
def index(request, name):
    if request.method != 'GET' and request.method != 'POST':
        return HttpResponse("This method is not allowed!\n")
    if name != 'file' and name != 'organism' and name != 'specimen' and name != 'dataset':
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
def get_protocols(request):
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
def get_protocol_details(request, id):
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
