#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author      patrick
# created on  2018/12/13

import re
import traceback
from datetime import datetime
import os


class SporttrtyPersist(object):


    INSERT_GAME_SQL ="""
        insert into game_result (game_name, host, vistor, score, half_score, win_rate, tie_rate, fail_rate, result, tc_id, time, league)
        values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    INSERT_GAME_DETAIL_SQL ="""
        insert into game_result_detail(game_id, goal_line, win_rate, tie_rate, fail_rate, time)
        values(%s, %s, %s, %s, %s, %s)
    """


    def process(self, infos, pool, fromDate, toDate):

        pid = os.getpid()

        print "SporttrtyPersist %s %s %s %s"%(pid, len(infos), fromDate, toDate)

        for info in infos:

            conn = pool.connection(False)
            cur = conn.cursor()

            teamRe = re.match(ur"([\u4E00-\u9FA5\w]+)\(([+-]?\d{1,3})\)?VS([\u4E00-\u9FA5\w]+)", info['team'])

            if teamRe is None:
                continue

            host, _, guest = teamRe.groups()

            scoreRe = re.match(r"(\d+):(\d+)", info['full'])
            if scoreRe is None:
                continue

            hostScore, guestScore = scoreRe.groups()
            hostScore, guestScore = int(hostScore), int(guestScore)

            if(int(hostScore) > int(guestScore)):
                res = 1
            elif(int(hostScore) < int(guestScore)):
                res = -1
            else:
                res = 0

            try:
                float(info['win_rate']), float(info['tie_rate']), float(info['fail_rate'])
                cur.execute(SporttrtyPersist.INSERT_GAME_SQL, (info['team'], host,
                                                          guest, info['full'], info['half'],
                                                          info['win_rate'], info['tie_rate'], info['fail_rate'],
                                                          res, '', datetime.strptime(info['time'], '%Y-%m-%d'), info['league']))
            except Exception,e:
                print "error param %s,%s"%(fromDate, toDate)
                traceback.print_exc()
                continue

            finally:
                cur.close()

            rowID = cur.lastrowid


            if info.has_key('hhad_his'):
                for odd in info['hhad_his']['odds']:
                    cur = conn.cursor()
                    cur.execute(SporttrtyPersist.INSERT_GAME_DETAIL_SQL,
                                (rowID, odd['goalline'], odd['h'], odd['d'], odd['a'],
                                datetime.strptime('%s %s'%(odd['date'], odd['time']), "%Y-%m-%d %H:%M:%S")))
                    cur.close()

            if info.has_key('had_his'):
               for odd in info['had_his']['odds']:
                    cur = conn.cursor()
                    cur.execute(SporttrtyPersist.INSERT_GAME_DETAIL_SQL,
                            (rowID, 0, odd['h'], odd['d'], odd['a'],
                             datetime.strptime('%s %s' % (odd['date'], odd['time']), "%Y-%m-%d %H:%M:%S")))
                    cur.close()

            conn.commit()
            conn.close()