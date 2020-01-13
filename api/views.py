import requests
import json

from django.http import JsonResponse, HttpResponse
from elasticsearch import Elasticsearch
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from django.core.cache import cache

from .helpers import generate_df, generate_df_for_breeds
from .constants import FIELD_NAMES, HUMAN_READABLE_NAMES


ALLOWED_INDICES = ['file', 'organism', 'specimen', 'dataset', 'experiment',
                   'protocol_files', 'protocol_samples', 'article',
                   'protocol_analysis', 'analysis', 'summary_organism',
                   'summary_specimen', 'summary_dataset', 'summary_file']


@csrf_exempt
def index(request, name):
    if request.method != 'GET' and request.method != 'POST':
        return HttpResponse("This method is not allowed!\n")
    if name not in ALLOWED_INDICES:
        return HttpResponse("This index doesn't exist!\n")

    # Parse request parameters
    size = request.GET.get('size', 10)
    field = request.GET.get('_source', '')
    sort = request.GET.get('sort', '')
    query = request.GET.get('q', '')

    set_cache = False
    data = None

    # Get cache if request goes to file or specimen
    # if int(size) == 100000 and query == '':
    #     cache_key = "{}_key".format(name)
    #     cache_time = 86400
    #     data = cache.get(cache_key)
    #     set_cache = True

    if not data:
        es = Elasticsearch([settings.NODE1, settings.NODE2])
        if request.body:
            data = es.search(index=name, size=size, body=json.loads(
                request.body.decode("utf-8")))
        else:
            if query != '':
                data = es.search(index=name, size=size, _source=field,
                                 sort=sort, q=query)
            else:
                data = es.search(index=name, size=size, _source=field,
                                 sort=sort)
        if set_cache:
            cache.set(cache_key, data, cache_time)

    return JsonResponse(data)


@csrf_exempt
def detail(request, name, id):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
    if name not in ALLOWED_INDICES:
        return HttpResponse("This index doesn't exist!\n")
    es = Elasticsearch([settings.NODE1, settings.NODE2])
    results = es.search(index=name, q="_id:{}".format(id))
    if results['hits']['total'] == 0:
        results = es.search(index=name, q="alternativeId:{}".format(id),
                            doc_type="_doc")
    return JsonResponse(results)


def fire_api(request, protocol_type, id):
    url = "https://{}.fire.sdo.ebi.ac.uk/fire/public/faang/ftp/protocols/" \
          "{}/{}".format(settings.DATACENTER, protocol_type, id)
    file = requests.get(url).content
    response = HttpResponse(file, content_type='application/pdf')
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(id)
    return response


def summary_api(request):
    final_results = ''
    for item in FIELD_NAMES.keys():
        data = requests.get(
            f"https://data.faang.org/api/summary_{item}/summary_{item}").json()
        data = data['hits']['hits'][0]['_source']
        results = list()
        results_faang_only = list()
        for field_name in FIELD_NAMES[item]:
            if 'breed' in field_name:
                tmp, tmp_faang_only = generate_df_for_breeds(
                    field_name, HUMAN_READABLE_NAMES[field_name], data)
            else:
                tmp, tmp_faang_only = generate_df(field_name,
                                                  HUMAN_READABLE_NAMES[
                                                      field_name], data)
            results.append(tmp)
            results_faang_only.append(tmp_faang_only)
        final_results += f'<h1>{item.capitalize()} Summary</h1>'
        final_results += '<br>'
        final_results += '<h3>FAANG only data</h3>'
        final_results += '<br>'
        for table in results_faang_only:
            final_results += table.to_html(index=False)
            final_results += '<br>'
        final_results += '<h3>All data</h3>'
        final_results += '<br>'
        for table in results:
            final_results += table.to_html(index=False)
            final_results += '<br>'
        final_results += '<br>'
        final_results += '<hr>'
    return HttpResponse(final_results)
