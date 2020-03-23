
import requests
from time import sleep
from db import redis_connect_string, db
from utils import get_url_from_redis
import redis
import datetime
from simpyder import Spider, FAKE_UA, SimpyderConfig
from bson import ObjectId
import logging


def update_interval(interval: int, key: str, value):
  now = datetime.datetime.now()
  return {
      'next': now,
      'date': now,
      'interval': interval,
      key: value,
  }


def update_video_interval(interval: int, bvid, aid):
  now = datetime.datetime.now()
  return {
      'next': now,
      'date': now,
      'interval': interval,
      'bvid': bvid,
      'aid': aid
  }


class BiliobRankAdd(Spider):

  def gen_url(self):
    online_url = 'https://api.bilibili.com/x/web-interface/online'
    url = 'https://api.bilibili.com/x/web-interface/ranking?rid={}&day=1&type=1&arc_type=0'
    try:
      online_data = self.get(online_url).json()
      rids = online_data['data']['region_count'].keys()
      online_data = self.get(url).json()
      for rid in rids:
        yield url.format(rid)
    except Exception as e:
      logging.exception(e)

  def parse(self, res):
    j = res.json()
    data = j['data']
    l = data['list']
    items = []
    for video_info in l:
      if 'aid' in video_info:
        aid = video_info['aid']
      else:
        aid = None
      bvid = video_info['bvid']
      mid = video_info['mid']
      item = {
          'bvid': bvid.lstrip("BV"),
          'aid': int(aid),
          'mid': int(mid)
      }
      items.append(item)
    return items

  def save(self, items):
    for item in items:
      db.author_interval.update_one({
          'mid': item['mid']},
          {
          '$set': update_interval(3600, 'mid', item['mid'])
      }, upsert=True)
      if item['aid'] != None:
        filter_dict = {'aid': item['aid']}
      else:
        filter_dict = {
            'bvid': item['bvid']}
      db.video_interval.update_one(filter_dict,
                                   {
                                       '$set': update_video_interval(3600, item['bvid'], item['aid'])
                                   }, upsert=True)
    return items


if __name__ == "__main__":
  s = BiliobRankAdd("biliob-rank-add")
  sc = SimpyderConfig()
  sc.PARSE_THREAD_NUMER = 1
  sc.DOWNLOAD_INTERVAL = 1
  sc.LOG_LEVEL = "INFO"
  sc.USER_AGENT = FAKE_UA
  s.set_config(sc)
  s.run()
