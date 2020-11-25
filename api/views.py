import requests
import json
import csv

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

ALLOWED_DOWNLOADS = ['file', 'organism', 'specimen', 'dataset']

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
    from_ = request.GET.get('from_', 0)
    # Example: {field1: [val1, val2], field2: [val1, val2], ...}
    filters = request.GET.get('filters', '{}')    
    # Example: {aggName1: field1, aggName2: field2, ...}  
    aggregations = request.GET.get('aggs', '{}')    

    # generate query for filtering
    filter_values = []
    not_filter_values = []
    filters = json.loads(filters)
    for key in filters.keys():
        if filters[key][0] != 'false':
            filter_values.append({"terms": {key: filters[key]}})
        else:
            not_filter_values.append({"terms": {key: ["true"]}})
    filter_val = {}
    if filter_values:
        filter_val['must'] = filter_values
    if not_filter_values:
        filter_val['must_not'] = not_filter_values
    if filter_val:
        filters = {"query": {"bool": filter_val}}

    # generate query for aggregations
    agg_values = {}
    aggregations = json.loads(aggregations)
    for key in aggregations.keys():
        # size determines number of aggregation buckets returned
        agg_values[key] = {"terms": {"field": aggregations[key], "size": 25}} 
        if key == 'paper_published':
            # aggregations for missing paperPublished field
            agg_values["paper_published_missing"] = {
                "missing": {"field": "paperPublished"}}

    filters['aggs'] = agg_values

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
                data = es.search(index=name, from_=from_, size=size, _source=field,
                                 sort=sort, q=query, body=filters)
            else:
                data = es.search(index=name, from_=from_, size=size, _source=field,
                                 sort=sort, body=filters)
        if set_cache:
            cache.set(cache_key, data, cache_time)

    return JsonResponse(data)

@csrf_exempt
def download(request, name):
    if request.method != 'GET':
        return HttpResponse("This method is not allowed!\n")
    if name not in ALLOWED_DOWNLOADS:
        return HttpResponse("This download doesn't exist!\n")

    # Request params
    SIZE = 1000000
    file_format = request.GET.get('file_format', '')
    field = request.GET.get('_source', '')
    column_names = request.GET.get('columns', '[]')
    sort = request.GET.get('sort', '')
    filters = request.GET.get('filters', '{}')
    
    columns = field.split(',')
    request_fields = []
    for col in columns:
        cols = col.split('.')
        if cols[0] == '_source':
            request_fields.append('.'.join(cols[1:]))
    request_fields = ','.join(request_fields)
    column_names = json.loads(column_names)

    # generate query for filtering
    filter_values = []
    not_filter_values = []
    filters = json.loads(filters)
    for key in filters.keys():
        if filters[key][0] != 'false':
            filter_values.append({"terms": {key: filters[key]}})
        else:
            not_filter_values.append({"terms": {key: ["true"]}})
    filter_val = {}
    if filter_values:
        filter_val['must'] = filter_values
    if not_filter_values:
        filter_val['must_not'] = not_filter_values
    if filter_val:
        filters = {"query": {"bool": filter_val}}

    # Get records from elasticsearch
    es = Elasticsearch([settings.NODE1, settings.NODE2])
    data = es.search(index=name, _source=request_fields, sort=sort, 
                     body=filters, size=SIZE)
    records = data['hits']['hits']

    # generate response payload
    if file_format == 'csv':
        filename = 'faang_data.csv'
        content_type = 'text/csv'
    else:
        filename = 'faang_data.txt'
        content_type = 'text/plain'
    response = HttpResponse(content_type=content_type)
    response['Content-Disposition'] = 'attachment; filename=' + filename

    # generate csv data
    writer = csv.DictWriter(response, fieldnames=columns)
    headers = {}
    i = 0
    for col in columns:
        headers[col] = column_names[i]
        i += 1
    writer.writerow(headers)
    for row in records:
        record = {}
        for col in columns:
            cols = col.split('.')
            record[col] = ''
            source = row
            for c in cols:
                if  isinstance(source, dict) and c in source.keys():
                    record[col] = source[c]
                    source = source[c]
                else:
                    record[col] = ''
                    break
        writer.writerow(record)
        
    # return formatted data
    if file_format == 'csv':
        return response
    else:
        # add padding to align with max length data in a column
        def space(i, d):
            max_len = len(max(list(zip(*row_data))[i], key=len))
            return d+b' '*(max_len-len(d))

        # create fixed width and '|' separated tabular text file
        data = response.content
        row_data = [i.rstrip(b'\r\n').split(b',') for i in filter(None, data.split(b'\n'))]
        align = [b' | '.join(space(*c) for c in enumerate(b)) for b in row_data]
        tab_data = b'\n'.join(align)
        response.content = tab_data
        return response

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
    if results['hits']['total'] == 0:
        results = es.search(index=name, q="biosampleId:{}".format(id),
                            doc_type="_doc")
    return JsonResponse(results)


def protocols_fire_api(request, protocol_type, id):
    url = "https://{}.fire.sdo.ebi.ac.uk/fire/public/faang/ftp/protocols/" \
          "{}/{}".format(settings.DATACENTER, protocol_type, id)
    file = requests.get(url).content
    response = HttpResponse(file, content_type='application/pdf')
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(id)
    return response


def trackhubregistry_fire_api(request, doc_id):
    url = "https://{}.fire.sdo.ebi.ac.uk/fire/public/faang/ftp/" \
          "trackhubregistry/{}".format(settings.DATACENTER, doc_id)
    file = requests.get(url).content
    response = HttpResponse(file, content_type='text/plain')
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(doc_id)
    return response


def trackhubregistry_with_dir_fire_api(request, genome_id, doc_id):
    url = "https://{}.fire.sdo.ebi.ac.uk/fire/public/faang/ftp/" \
          "trackhubregistry/{}/{}".format(settings.DATACENTER, genome_id,
                                          doc_id)
    file = requests.get(url).content
    response = HttpResponse(file, content_type='text/plain')
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(doc_id)
    return response


def trackhubregistry_with_dirs_fire_api(request, genome_id, folder, doc_id):
    url = "https://{}.fire.sdo.ebi.ac.uk/fire/public/faang/ftp/" \
          "trackhubregistry/{}/{}/{}".format(settings.DATACENTER, genome_id,
                                             folder, doc_id)
    file = requests.get(url).content
    response = HttpResponse(file, content_type='text/plain')
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(doc_id)
    return response


def summary_api(request):
    final_results = ''
    for item in FIELD_NAMES.keys():
        data = requests.get(
            "https://data.faang.org/api/summary_{}/summary_{}".format(
                item, item)).json()
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
        final_results += '<h1>{} Summary</h1>'.format(item.capitalize())
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
