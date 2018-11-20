from django.http import HttpResponse
from elasticsearch import Elasticsearch
from django.views.decorators.csrf import csrf_exempt
import json


STAGING_NODE1 = 'wp-np3-e2:9200'
STAGING_NODE2 = 'wp-np3-e3:9200'


@csrf_exempt
def index(request, name):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
    if name != 'file' and name != 'organism' and name != 'specimen' and name != 'dataset':
        return HttpResponse("This method is not allowed!\n")
    size = request.GET.get('size', 10)
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2])
    if request.body:
        results = es_staging.search(index=name, size=size, body=json.loads(request.body))
    else:
        field = request.GET.get('_source', '')
        sort = request.GET.get('sort', '')
        query = request.GET.get('q', '')
        if query != '':
            results = es_staging.search(index=name, size=size, _source=field, sort=sort, q=query)
        else:
            results = es_staging.search(index=name, size=size, _source=field, sort=sort)
    results = json.dumps(results)
    return HttpResponse(results)


@csrf_exempt
def detail(request, name, id):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2])
    results = es_staging.search(index=name, q="_id:{}".format(id))
    results = json.dumps(results)
    return HttpResponse(results)
