from django.http import HttpResponse
from elasticsearch import Elasticsearch
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import json

ALLOWED_INDICES = ['file', 'organism', 'specimen', 'dataset', 'experiment', 'protocol_files', 'protocol_samples']


@csrf_exempt
def index(request, name):
    if request.method != 'GET' and request.method != 'POST':
        return HttpResponse("This method is not allowed!\n")
    if name not in ALLOWED_INDICES:
        return HttpResponse("This index doesn't exist!\n")
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
        results = es.search(index=name, q="alternativeId:{}".format(id), doc_type="_doc")
    results = json.dumps(results)
    return HttpResponse(results)
