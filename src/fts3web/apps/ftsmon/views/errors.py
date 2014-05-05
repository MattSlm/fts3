# Copyright notice:
# Copyright (C) Members of the EMI Collaboration, 2010.
#
# See www.eu-emi.eu for details on the copyright holders
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.db.models import Q, Count, Avg
from django.http import Http404
from django.shortcuts import render, redirect
from ftsweb.models import File
from jsonify import jsonify, jsonify_paged
from util import paged


@jsonify_paged
def showErrors(httpRequest):
    time_window = timedelta(hours = 1)
    if httpRequest.GET.get('time_window', None):
        try:
            time_window = timedelta(hours = int(httpRequest.GET['time_window']))
        except:
            pass
    
    notBefore = datetime.utcnow() - time_window
    errors = File.objects.filter(file_state = 'FAILED', job_finished__gte = notBefore)

    if httpRequest.GET.get('source_se', None):
        errors = errors.filter(source_se = httpRequest.GET['source_se'])
    if httpRequest.GET.get('dest_se', None):
        errors = errors.filter(dest_se = httpRequest.GET['dest_se'])
    if httpRequest.GET.get('vo', None):
        errors = errors.filter(vo_name = httpRequest.GET['vo'])
    if httpRequest.GET.get('reason', None):
        errors = errors.filter(reason__icontains = httpRequest.GET['reason'])
                         
    errors = errors.values('source_se', 'dest_se')\
                         .annotate(count = Count('file_state'))\
                         .order_by('-count')
    # Fetch all first to avoid 'count' query
    return list(errors.all())


@jsonify
def errorsForPair(httpRequest):
    source_se = httpRequest.GET.get('source_se', None)
    dest_se   = httpRequest.GET.get('dest_se', None)
    reason    = httpRequest.GET.get('reason', None)
    
    if not source_se or not dest_se:
        raise Http404
    
    time_window = timedelta(hours = 1)
    if httpRequest.GET.get('time_window', None):
        try:
            time_window = timedelta(hours = int(httpRequest.GET['time_window']))
        except:
            pass

    notBefore = datetime.utcnow() - time_window
    transfers = File.objects.filter(file_state = 'FAILED',
                                    job_finished__gte = notBefore,
                                    source_se = source_se, dest_se = dest_se)
    if reason:
        transfers = transfers.filter(reason__icontains = reason)
    
    transfers = transfers.values('vo_name', 'reason')
    transfers = transfers.annotate(count = Count('reason'))
    transfers = transfers.order_by('-count')
    # Trigger query to fetch all
    transfers = list(transfers)
    
    # Count by error type
    classification = dict()
    for t in transfers:
        type = t['reason'].split()[0]
        if type.isupper():
            classification[type] = classification.get(type, 0) + t['count']
                            
    return {
        'errors': paged(transfers, httpRequest),
        'classification': classification
    }
