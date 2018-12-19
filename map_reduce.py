#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author      patrick
# created on  2018/12/12

from spider.sporttery import SportterySpider
from persistence.sporttery import SporttrtyPersist
import os

class ResMapReduce:

    @staticmethod
    def process(fromDate, toDate, queue, pool):

        pid = os.getpid()

        print "%s fetch begin %s %s" %(pid, fromDate, toDate)

        game = SportterySpider().fetch(start = fromDate.strftime("%Y-%m-%d"), end = toDate.strftime("%Y-%m-%d"))

        print "%s fetch over" % pid

        print "%s insert begin" % pid

        SporttrtyPersist().process(game, pool, fromDate, toDate)


        print "%s insert over" % pid





