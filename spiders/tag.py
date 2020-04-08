
import requests
from time import sleep
from db import redis_connect_string, db
from utils import get_url_from_redis
import redis
import datetime
from simpyder import Spider, FAKE_UA, SimpyderConfig
from bson import ObjectId
import logging
from utils import enc, dec


class BiliobTagSpider(Spider):
  def gen_url(self):
    while True:
      try:
        videos = db.video.find({
            '$or': [{'tag': []}, {'tag': {
                '$exists': False,
            }}]
        }, {'aid': 1, 'bvid': 1}).limit(100)
        for each_video in videos:
          if 'bvid' in each_video:
            bvid = each_video['bvid']
          else:
            bvid = enc(each_video['aid'])
          yield 'https://www.bilibili.com/video/BV{}'.format(bvid)
      except Exception as e:
        logging.exception(e)
      sleep(10)

  def parse(self, res):
    bvid = str(
        res.url.lstrip(
            'https://www.bilibili.com/video/BV').rsplit('?')[0])
    tagName = res.xpath("//li[@class='tag']/a/text()")
    item = {}
    item['bvid'] = bvid
    item['tag_list'] = []
    item['aid'] = dec('BV' + bvid)
    if tagName != []:
      ITEM_NUMBER = len(tagName)
      for i in range(0, ITEM_NUMBER):
        item['tag_list'].append(tagName[i])
    return item

  def save(self, item):
    if item['tag_list'] == []:
      item['tag_list'] = [None]
    if db.video.find_one({'bvid': item['bvid']}, {'bvid': item['bvid']}) != None:
      db.video.update_one({
          'bvid': item['bvid']
      }, {
          '$set': {
              'aid': item['aid'],
              'tag': item['tag_list']
          }
      }, upsert=True)
    else:
      db.video.update_one({
          'aid': item['aid']
      }, {
          '$set': {
              'bvid': item['bvid'],
              'tag': item['tag_list']
          }
      }, upsert=True)
    return item


s = BiliobTagSpider("标签爬虫")

sc = SimpyderConfig()
sc.PARSE_THREAD_NUMER = 8
sc.LOG_LEVEL = "INFO"
sc.USER_AGENT = FAKE_UA
s.set_config(sc)
if __name__ == "__main__":
  s.run()
