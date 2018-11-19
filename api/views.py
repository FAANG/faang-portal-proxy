from django.http import HttpResponse
from elasticsearch import Elasticsearch
import json
from django.views.decorators.csrf import csrf_exempt

STAGING_NODE1 = 'wp-np3-e2:9200'
STAGING_NODE2 = 'wp-np3-e3:9200'


@csrf_exempt
def index(request, name):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
    if name != 'file' and name != 'organism' and name != 'specimen' and name != 'dataset':
        return HttpResponse("This method is not allowed!\n")
    size = request.GET.get('size', 100000)
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2])
    if request.body:
        results = es_staging.search(index=name, body=json.loads(request.body), size=size)
    else:
        results = es_staging.search(index=name, size=size)
    results = json.dumps(results)
    return HttpResponse(results)
