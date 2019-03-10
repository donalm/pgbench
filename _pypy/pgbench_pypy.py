#!/usr/bin/env python3
#
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from psycopg2cffi import compat
compat.register()

import os
import sys
sys.path.insert(0, "/home/donal/Geek/thrum")
import math
import time
import struct
import traceback

from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import task
from twisted.python import log
from twisted.python import util
from termcolor import cprint

from thrum.twisted import connection
from thrum.twisted import cpool
from thrum import binary

import argparse
import csv
import io
import json
import re
import sys
import time

import numpy as np

from txpostgres import txpostgres

class PatchedConnection(txpostgres.Connection):
    """Postgres Connection for Async."""

    def connect(self, *args, **kwargs):
        """
        """
        if self.detector:
            self.detector.setReconnectable(self, *args, **kwargs)

        if self._connection and not self._connection.closed:
            return defer.fail(txpostgres.AlreadyConnected())

        kwargs['async_'] = True
        try:
            self._connection = self.connectionFactory(*args, **kwargs)
        except Exception:
            return defer.fail()

        def startReadingAndPassthrough(ret):
            """Add socket to reactor."""
            self.reactor.addReader(self)
            return ret

        # The connection is always a reader in the reactor, to receive NOTIFY
        # events immediately when they're available.
        d = self.poll()
        return d.addCallback(startReadingAndPassthrough)

@defer.inlineCallbacks

def txpostgres_connect(args):
    credentials = {
        "user": args.pguser,
        "port": args.pgport,
        "host": args.pghost
    }
    xcredentials = {
        "dbname": "assemblyelementssessions",
        "user": "assemblyelementssessions",
        "password": "AssembyElementsSessions19283218231",
        "port": 5432,
        "host": "localhost"
    }
    conn = PatchedConnection()
    yield conn.connect(**credentials)
    defer.returnValue(conn)

def txpostgres_execute(conn, query, args):
    return conn.runQuery(query, args).addCallback(len)

@defer.inlineCallbacks
def worker(executor, eargs, start, duration, timeout):
    queries = 0
    rows = 0
    latency_stats = np.zeros((timeout * 100,))
    min_latency = float('inf')
    max_latency = 0.0

    end_time = start + duration
    while time.monotonic() < end_time:
        req_start = time.monotonic()
        rows += yield executor(*eargs)
        req_time = round((time.monotonic() - req_start) * 1000 * 100)

        if req_time > max_latency:
            max_latency = req_time
        if req_time < min_latency:
            min_latency = req_time
        latency_stats[req_time] += 1
        queries += 1

    defer.returnValue((queries, rows, latency_stats, min_latency, max_latency))

@defer.inlineCallbacks
def runner(reactor, args, connector, executor, copy_executor, is_async,
                 arg_format, query, query_args, setup, teardown):

    timeout = args.timeout * 1000
    concurrency = args.concurrency

    if arg_format == 'python':
        query = re.sub(r'\$\d+', '%s', query)

    is_copy = query.startswith('COPY ')

    if is_copy:
        if copy_executor is None:
            raise RuntimeError('COPY is not supported for {}'.format(executor))
        executor = copy_executor

        match = re.match('COPY (\w+)\s*\(\s*((?:\w+)(?:,\s*\w+)*)\s*\)', query)
        if not match:
            raise RuntimeError('could not parse COPY query')

        query_info = query_args[0]
        query_args[0] = [query_info['row']] * query_info['count']
        query_args.append({
            'table': match.group(1),
            'columns': [col.strip() for col in match.group(2).split(',')]
        })

    conns = []

    for i in range(concurrency):
        if is_async:
            conn = yield connector(args)
        else:
            conn = connector(args)
        conns.append(conn)

    @defer.inlineCallbacks
    def _do_run(run_duration):
        start = time.monotonic()

        tasks = []

        # Asyncio driver
        for i in range(concurrency):
            task = worker(executor, [conns[i], query, query_args],
                          start, run_duration, timeout)
            tasks.append(task)

        results = yield defer.gatherResults(tasks)

        end = time.monotonic()

        defer.returnValue((results, end - start))

    if setup:
        admin_conn = conns[0]
        yield admin_conn.runOperation(setup, None)

    try:
        try:
            if args.warmup_time:
                yield _do_run(args.warmup_time)

            results, duration = yield _do_run(args.duration)
        finally:
            for conn in conns:
                yield conn.close()

        min_latency = float('inf')
        max_latency = 0.0
        queries = 0
        rows = 0
        latency_stats = None

        for result in results:
            t_queries, t_rows, t_latency_stats, t_min_latency, t_max_latency =\
                result
            queries += t_queries
            rows += t_rows
            if latency_stats is None:
                latency_stats = t_latency_stats
            else:
                latency_stats = np.add(latency_stats, t_latency_stats)
            if t_max_latency > max_latency:
                max_latency = t_max_latency
            if t_min_latency < min_latency:
                min_latency = t_min_latency

        if is_copy:
            copyargs = query_args[-1]

            rowcount = yield admin_conn.fetchval('''
                SELECT
                    count(*)
                FROM
                    "{tabname}"
            '''.format(tabname=copyargs['table']))

            print(rowcount, file=sys.stderr)

            if rowcount < len(query_args[0]) * queries:
                raise RuntimeError(
                    'COPY did not insert the expected number of rows')

        data = {
            'queries': queries,
            'rows': rows,
            'duration': duration,
            'min_latency': min_latency,
            'max_latency': max_latency,
            'latency_stats': latency_stats.tolist(),
            'output_format': args.output_format
        }

    finally:
        if teardown:
            admin_conn = yield connector(args)
            yield admin_conn.runOperation(teardown, None)
            yield admin_conn.close()

    print(json.dumps(data))


def die(msg):
    print('fatal: {}'.format(msg), file=sys.stderr)
    sys.exit(1)

def okgo(reactor):
    df = defer.succeed(1)
    return df

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='async pg driver benchmark [concurrent]')
    parser.add_argument(
        '-C', '--concurrency', type=int, default=10,
        help='number of concurrent connections')
    parser.add_argument(
        '-D', '--duration', type=int, default=30,
        help='duration of test in seconds')
    parser.add_argument(
        '--timeout', default=2, type=int,
        help='server timeout in seconds')
    parser.add_argument(
        '--warmup-time', type=int, default=5,
        help='duration of warmup period for each benchmark in seconds')
    parser.add_argument(
        '--output-format', default='text', type=str,
        help='output format', choices=['text', 'json'])
    parser.add_argument(
        '--pghost', type=str, default='127.0.0.1',
        help='PostgreSQL server host')
    parser.add_argument(
        '--pgport', type=int, default=5432,
        help='PostgreSQL server port')
    parser.add_argument(
        '--pguser', type=str, default='postgres',
        help='PostgreSQL server user')
    parser.add_argument(
        'driver', help='driver implementation to use',
        choices=['aiopg', 'txpostgres', 'psycopg', 'postgresql'])
    parser.add_argument(
        'queryfile', help='file to read benchmark query information from')

    args = parser.parse_args()

    if args.queryfile == '-':
        querydata_text = sys.stdin.read()
    else:
        with open(args.queryfile, 'rt') as f:
            querydata_text = f.read()

    querydata = json.loads(querydata_text)

    query = querydata.get('query')
    if not query:
        die('missing "query" in query JSON')

    query_args = querydata.get('args')
    if not query_args:
        query_args = []

    setup = querydata.get('setup')
    teardown = querydata.get('teardown')
    if setup and not teardown:
        die('"setup" is present, but "teardown" is missing in query JSON')

    copy_executor = None

    if args.driver == 'aiopg':
        if query.startswith('COPY '):
            connector, executor, copy_executor = \
                psycopg_connect, psycopg_execute, psycopg_copy
            is_async = False
        else:
            connector, executor = aiopg_connect, aiopg_execute
            is_async = True
        arg_format = 'python'
    elif args.driver == 'txpostgres':
        connector, executor = txpostgres_connect, txpostgres_execute
        is_async = True
        arg_format = 'python'
    elif args.driver == 'psycopg':
        connector, executor, copy_executor = \
            psycopg_connect, psycopg_execute, psycopg_copy
        is_async = False
        arg_format = 'python'
    elif args.driver == 'postgresql':
        connector, executor = pypostgresql_connect, pypostgresql_execute
        is_async = False
        arg_format = 'native'
    else:
        raise ValueError('unexpected driver: {!r}'.format(args.driver))

    task.react(runner, (args, connector, executor, copy_executor, is_async,
                         arg_format, query, query_args, setup, teardown))

