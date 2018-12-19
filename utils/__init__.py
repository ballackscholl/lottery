#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author      patrick
# created on  2018/12/6

import urllib
import urllib2
import socket

def connectFormHttp(url, data, header={}, isPost=False, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
    header['Content-type'] = 'application/x-www-form-urlencoded'
    if isPost:
        if data is not None:
            data = urllib.urlencode(data)
        request = urllib2.Request(url, data, headers=header)
    else:
        if data is not None:
            url = ("%s?%s")%(url, urllib.urlencode(data))
        request = urllib2.Request(url, headers=header)

    opener = urllib2.build_opener()
    connector = opener.open(request, timeout=timeout)
    ret = connector.read()
    connector.close()
    return ret


def startProcesses(processes):
    map( lambda process : process.start(), processes)


def joinProcesses(processes):

    joiner = []
    joiner.extend(processes)

    while(len(joiner) > 0):
        removers = []
        for item in joiner:
            item.join()
            if not item.is_alive():
                removers.append(item)

        if len(removers) > 0:
            for remover in removers:
                joiner.remove(remover)