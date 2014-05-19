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
from django.db import connection
from django.db.models import Count, Avg
import types
import sys

from ftsweb.models import File
from jobs import setup_filters
from jsonify import jsonify
from util import get_order_by, paged
import settings


def _db_to_date():
    if settings.DATABASES['default']['ENGINE'] == 'django.db.backends.oracle':
        return 'TO_DATE(%s, \'YYYY-MM-DD HH24:MI:SS\')'
    elif settings.DATABASES['default']['ENGINE'] == 'django.db.backends.mysql':
        return 'STR_TO_DATE(%s, \'%%Y-%%m-%%d %%H:%%i:%%S\')'
    else:
        return '%s'


def _db_limit(sql, limit):
    if settings.DATABASES['default']['ENGINE'] == 'django.db.backends.oracle':
        return "SELECT * FROM (%s) WHERE rownum <= %d" % (sql, limit)
    elif settings.DATABASES['default']['ENGINE'] == 'django.db.backends.mysql':
        return sql + " LIMIT %d" % limit
    else:
        return sql


def _get_pair_limits(limits, source, destination):
    pair_limits = {'source': dict(), 'destination': dict()}
    for l in limits:
        if l[0] == source:
            if l[2]:
                pair_limits['source']['bandwidth'] = l[2]
            elif l[3]:
                pair_limits['source']['active'] = l[3]
        elif l[1] == destination:
            if l[2]:
                pair_limits['destination']['bandwidth'] = l[2]
            elif l[3]:
                pair_limits['destination']['active'] = l[3]
    if len(pair_limits['source']) == 0:
        del pair_limits['source']
    if len(pair_limits['destination']) == 0:
        del pair_limits['destination']
    return pair_limits


def _get_pair_udt(udt_pairs, source, destination):
    for u in udt_pairs:
        if udt_pairs[0] == source and udt_pairs[1] == destination:
            return True
        elif udt_pairs[0] == source and not udt_pairs[1]:
            return True
        elif not udt_pairs[0] and udt_pairs[1] == destination:
            return True
    return False


class OverviewExtended(object):
    """
    Wraps the return of overview, so when iterating, we can retrieve
    additional information.
    This way we avoid doing it for all items. Only those displayed will be queried
    (i.e. paging)
    """

    def __init__(self, not_before, objects):
        self.objects = objects
        self.not_before = not_before

    def __len__(self):
        return len(self.objects)

    def _get_avg_duration(self, source, destination, vo):
        avg_duration = File.objects.filter(source_se=source, dest_se=destination, vo_name=vo) \
            .filter(job_finished__gte=self.not_before, file_state='FINISHED') \
            .aggregate(Avg('tx_duration'))
        return avg_duration['tx_duration__avg']

    @staticmethod
    def _get_avg_queued(source, destination, vo):
        if settings.DATABASES['default']['ENGINE'] == 'django.db.backends.oracle':
            avg_queued_query = """
            SELECT AVG(EXTRACT(HOUR FROM time_diff) * 3600 + EXTRACT(MINUTE FROM time_diff) * 60 + EXTRACT(SECOND FROM time_diff))
            FROM (SELECT (t_file.start_time - t_job.submit_time) AS time_diff
              FROM t_file, t_job
              WHERE t_job.job_id = t_file.job_id AND
                    t_file.source_se = %s AND t_file.dest_se = %s AND t_job.vo_name = %s AND
                    t_job.job_finished IS NULL AND t_file.job_finished IS NULL AND
                    t_file.start_time IS NOT NULL
            )
            """
        else:
            avg_queued_query = """
            SELECT AVG(TIMESTAMPDIFF(SECOND, t_job.submit_time, t_file.start_time)) FROM t_file, t_job
            WHERE t_job.job_id = t_file.job_id AND
                t_file.source_se = %s AND t_file.dest_se = %s AND t_job.vo_name = %s AND
                t_job.job_finished IS NULL AND t_file.job_finished IS NULL AND
                t_file.start_time IS NOT NULL
            ORDER BY t_file.start_time DESC LIMIT 5
            """

        cursor = connection.cursor()
        cursor.execute(avg_queued_query, (source, destination, vo))
        avg = cursor.fetchall()
        if len(avg) > 0 and avg[0][0] is not None:
            return int(avg[0][0])
        else:
            return None

    def _get_frequent_error(self, source, destination, vo):
        reason = File.objects.filter(source_se=source, dest_se=destination, vo_name=vo) \
            .filter(job_finished__gte=self.not_before, file_state='FAILED') \
            .values('reason').annotate(count=Count('reason')).values('reason', 'count').order_by('-count')
        if len(reason) > 0:
            return "[%(count)d] %(reason)s" % reason[0]
        else:
            return None

    def __getitem__(self, indexes):
        if isinstance(indexes, types.SliceType):
            return_list = self.objects[indexes]
            for item in return_list:
                item['avg_duration'] = self._get_avg_duration(item['source_se'], item['dest_se'], item['vo_name'])
                item['avg_queued'] = self._get_avg_queued(item['source_se'], item['dest_se'], item['vo_name'])
                item['most_frequent_error'] = self._get_frequent_error(item['source_se'], item['dest_se'],
                                                                       item['vo_name'])
            return return_list
        else:
            return self.objects[indexes]


@jsonify
def get_overview(http_request):
    filters = setup_filters(http_request)
    if filters['time_window']:
        not_before = datetime.utcnow() - timedelta(hours=filters['time_window'])
    else:
        not_before = datetime.utcnow() - timedelta(hours=1)

    cursor = connection.cursor()

    # Get all pairs first
    pairs_query = "SELECT DISTINCT source_se, dest_se FROM t_file WHERE 1=1"
    pairs_params = []
    if filters['source_se']:
        pairs_query += " AND source_se = %s"
        pairs_params.append(filters['source_se'])
    if filters['dest_se']:
        pairs_query += " AND dest_se = %s"
        pairs_params.append(filters['dest_se'])

    triplets = {}

    cursor.execute(pairs_query, pairs_params)
    for (source, dest) in cursor.fetchall():
        query = """
        SELECT file_state, vo_name, count(file_id),
               SUM(CASE WHEN file_state = 'ACTIVE' THEN throughput ELSE 0 END)
        FROM t_file
        WHERE (job_finished IS NULL OR job_finished >= %s)
              AND source_se = %%s AND dest_se = %%s
              AND file_state != 'NOT_USED'
        """ % _db_to_date()
        params = [not_before.strftime('%Y-%m-%d %H:%M:%S'),
                  source, dest]
        if filters['vo']:
            query += " AND vo_name = %s"
            params.append(filters['vo'])
        query += " GROUP BY file_state, vo_name ORDER BY NULL"

        all_vos = []
        cursor.execute(query, params)
        for row in cursor.fetchall():
            all_vos.append(row[1])
            triplet_key = (source, dest, row[1])
            triplet = triplets.get(triplet_key, dict())
            triplet[row[0].lower()] = row[2]
            if row[3]:
                triplet['current'] = triplet.get('current', 0) + row[3]
            triplets[triplet_key] = triplet

        # Snapshot for the pair
        # Can not get it per VO :(
        query = _db_limit("""
        SELECT agrthroughput
        FROM t_optimizer_evolution
        WHERE source_se = %%s AND dest_se = %%s
            AND datetime >= %s
        ORDER BY datetime DESC
        """ % _db_to_date(), 1)

        params = [source, dest, not_before.strftime('%Y-%m-%d %H:%M:%S')]

        cursor.execute(query, params)
        aggregated_total = cursor.fetchall()
        if len(aggregated_total):
            aggregated_total = aggregated_total[0][0]
            if aggregated_total:
                for vo in all_vos:
                    triplet_key = (source, dest, vo)
                    triplets[triplet_key]['aggregated'] = aggregated_total


    # Limitations
    limit_query = "SELECT source_se, dest_se, throughput, active FROM t_optimize WHERE throughput IS NOT NULL or active IS NOT NULL"
    cursor.execute(limit_query)
    limits = cursor.fetchall()

    # UDT
    udt_query = "SELECT source_se, dest_se FROM t_optimize WHERE udt = 'on'"
    cursor.execute(udt_query)
    udt_pairs = cursor.fetchall()

    # Transform into a list
    objs = []
    for (triplet, obj) in triplets.iteritems():
        obj['source_se'] = triplet[0]
        obj['dest_se'] = triplet[1]
        obj['vo_name'] = triplet[2]
        if 'current' not in obj and 'active' in obj:
            obj['current'] = 0
        failed = obj.get('failed', 0)
        finished = obj.get('finished', 0)
        total = failed + finished
        if total > 0:
            obj['rate'] = (finished * 100.0) / total
        # Append limit, if any
        obj['limits'] =_get_pair_limits(limits, triplet[0], triplet[1])
        # Mark UDT-enabled
        obj['udt'] = _get_pair_udt(udt_pairs, triplet[0], triplet[1])

        objs.append(obj)

    # Ordering
    (order_by, order_desc) = get_order_by(http_request)

    if order_by == 'active':
        sorting_method = lambda o: (o.get('active', 0), o.get('submitted', 0))
    elif order_by == 'finished':
        sorting_method = lambda o: (o.get('finished', 0), o.get('failed', 0))
    elif order_by == 'failed':
        sorting_method = lambda o: (o.get('failed', 0), o.get('finished', 0))
    elif order_by == 'canceled':
        sorting_method = lambda o: (o.get('canceled', 0), o.get('finished', 0))
    elif order_by == 'throughput':
        if order_desc:
            # NULL current first (so when reversing, they are last)
            sorting_method = lambda o: (o.get('current', None), o.get('active', 0))
        else:
             # NULL current last
            sorting_method = lambda o: (o.get('current', sys.maxint), o.get('active', 0))
    elif order_by == 'aggregated':
        if order_desc:
            # NULL current first (so when reversing, they are last)
            sorting_method = lambda o: (o.get('aggregated', None), o.get('active', 0))
        else:
             # NULL current last
            sorting_method = lambda o: (o.get('aggregated', sys.maxint), o.get('active', 0))
    elif order_by == 'rate':
        sorting_method = lambda o: (o.get('rate', 0), o.get('finished', 0))
    else:
        sorting_method = lambda o: (o.get('submitted', 0), o.get('active', 0))

    # Generate summary
    summary = {
        'submitted': sum(map(lambda o: o.get('submitted', 0), objs), 0),
        'active': sum(map(lambda o: o.get('active', 0), objs), 0),
        'finished': sum(map(lambda o: o.get('finished', 0), objs), 0),
        'failed': sum(map(lambda o: o.get('failed', 0), objs), 0),
        'canceled': sum(map(lambda o: o.get('canceled', 0), objs), 0),
        'current': sum(map(lambda o: o.get('current', 0), objs), 0),
        'aggregated': sum(map(lambda o: o.get('aggregated', 0), objs), 0),
    }
    if summary['finished'] > 0 or summary['failed'] > 0:
        summary['rate'] = (float(summary['finished']) / (summary['finished'] + summary['failed'])) * 100

    # Return
    return {
        'overview': paged(
            OverviewExtended(not_before, sorted(objs, key=sorting_method, reverse=order_desc)),
            http_request
        ),
        'summary': summary
    }
