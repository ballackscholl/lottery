#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author      patrick
# created on  2018/12/6

import utils
import bs4
import json
import time
import os
import re

class SportterySpider(object):

    HOME = 'http://info.sporttery.cn/football/match_result.php?page=%s&search_league=0&start_date=%s&end_date=%s&dan='

    def fetch(self, *args, **kwargs):

        pid = os.getpid()

        tryTimes = 5

        games = []
        index = 1
        while(True):

            url = SportterySpider.HOME%(index, kwargs['start'], kwargs['end'])
            while (True):

                try:
                    ret = utils.connectFormHttp(url, None,  timeout=10)
                except Exception, e:
                    print "%s HOME game error %s %s %s" % (pid, kwargs['start'], kwargs['end'], index)
                    time.sleep(1)
                    continue
                else:
                    break

            index += 1

            soup = bs4.BeautifulSoup(ret, 'lxml')

            pageNumItem = soup.find('span', attrs={"class": "u-fl"})

            pageNum = pageNumItem.span.text


            for child in soup.find(attrs={"class": "m-tab"}).children:
                if isinstance(child, bs4.element.Tag):
                    if not child.attrs.has_key('style'):
                        gameInfo = child.find_all('td')

                        #print gameInfo[10].a.attrs['href'].lstrip('pool_result.php?id=')

                        detailUrl = gameInfo[10].a.attrs['href']
                        mid = re.match(ur".*\?id=(\d+)$",detailUrl)

                        if(mid):
                            mid = mid.groups()[0]

                        game = {
                            'time': gameInfo[0].text,
                            'name': gameInfo[1].text,
                            'league': gameInfo[2].text,
                            'team': gameInfo[3].text,
                            'half': gameInfo[4].text,
                            'full': gameInfo[5].text,
                            'win_rate': gameInfo[6].text,
                            'tie_rate': gameInfo[7].text,
                            'fail_rate': gameInfo[8].text,
                            'mid' : mid
                        }


                        tryIndex = 0
                        ret = None

                        while(tryIndex <= tryTimes):
                            tryIndex += 1
                            try:
                                ret = utils.connectFormHttp(
                                    "http://i.sporttery.cn/api/fb_match_info/get_pool_rs/?mid=" + game['mid'], None, timeout=3)
                                time.sleep(0.2)
                            except Exception,e:
                                print "%s fetch game error mid %s"%(pid, game['mid'])
                                time.sleep(1)
                                continue
                            else:
                                break

                        if ret is not None:
                            ret = json.loads(ret)

                            #print "%s fetch game mid %s"%(pid, game['mid'])

                            if ret['status']['code'] == 0:
                                try:
                                    game['had'] = ret['result']['pool_rs']['had']
                                    game['hhad'] = ret['result']['pool_rs']['hhad']
                                    game['had_his'] = ret['result']['odds_list']['had']
                                    game['hhad_his'] = ret['result']['odds_list']['hhad']
                                except KeyError :
                                    pass
                                except TypeError:
                                    pass

                        games.append(game)

            if len(games) >= int(pageNum):
                print  "%s fetch game end %s" % (pid, len(games))
                break

        return games
