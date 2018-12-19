#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author      patrick
# created on  2018/12/6
from mysql import connector
from DBUtils.PooledDB import PooledDB




from datetime import datetime, timedelta

from multiprocessing import Process, Manager, cpu_count

from map_reduce import ResMapReduce

import utils

if __name__ == '__main__':

    #'2015-01-01'
    cpuCount = 1

    fromDate = datetime.strptime('2018-12-18', '%Y-%m-%d')

    toDate = datetime.strptime('2018-12-18', '%Y-%m-%d')

    diff = toDate - fromDate

    step = diff.days / cpuCount

    index = 0
    peers = []
    cur = fromDate
    while(index < cpuCount):
        if(index + 1 == cpuCount):
            peers.append((cur, toDate))
        else:
            peers.append((cur, cur + timedelta(days=(step-1))))

        cur = cur + timedelta(days=step)
        index += 1

    print peers


    config = {
        'host': '127.0.0.1',
        'user': 'zhouyu',
        'password': 'zhouyu',
        'port': '3306',
        'database': 'lottery',
        'charset': 'utf8'
    }

    pool = PooledDB(creator=connector, mincached=1, maxcached=10,
                    maxconnections=10, blocking=True,
                    **config)

    processes = map(lambda peer: Process(target=ResMapReduce.process, args=(peer[0], peer[1], None, pool)), peers)

    print processes


    ps = [Process(target=ResMapReduce.process, args=(fromDate, toDate, None, pool))]

    utils.startProcesses(processes)
    utils.joinProcesses(processes)
