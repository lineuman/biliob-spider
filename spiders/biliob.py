import requests
from time import sleep
from db import redis_connect_string, db
from utils import get_url_from_redis
import redis
import datetime
from simpyder import Spider
from simpyder import FAKE_UA
from simpyder import SimpyderConfig
from bson import ObjectId
from utils import sub_channel_2_channel


class BiliobSpider(Spider):

  def __init__(self, name, thread=1, interval=0.15):
    super().__init__()
    self.name = name
    self.db = db
    sc = SimpyderConfig()
    sc.USER_AGENT = FAKE_UA
    sc.PARSE_THREAD_NUMER = thread
    sc.LOG_LEVEL = "INFO"
    sc.DOWNLOAD_INTERVAL = interval
    self.set_config(sc)

  def mid_gener(self):
    while True:
      try:
        # 如果存在锁
        if self.db.lock.count_documents({"name": "author_interval"}):
          sleep(0.1)
          continue
        # 挂锁
        self.db.lock.insert(
            {"name": "author_interval", "date": datetime.datetime.utcnow()})

        # 先检查有没有手动操作
        data = list(self.db.author_interval.find(
            {'order': {'$exists': 1, '$ne': []}}).limit(100))
        if data == []:
          data = list(self.db.author_interval.find(
              {'next': {'$lt': datetime.datetime.utcnow()}}).limit(100))
        else:
          # 如果存在手动操作，则刷新数据
          for each_data in data:
            for order_id in each_data['order']:
              self.db.user_record.update_one({'_id': order_id}, {'$set': {
                  'isExecuted': True
              }})
        if data != None:
          for each_data in data:
            each_data['next'] = each_data['next'] + \
                datetime.timedelta(seconds=each_data['interval'])
            each_data['order'] = []
            self.db.author_interval.update_one(
                {'mid': each_data['mid']}, {'$set': each_data})
        # 解锁
        self.db.lock.delete_one(
            {"name": "author_interval"})
        for each_data in data:
          yield each_data['mid']
      except Exception as e:
        self.logger.exception(e)

  def video_gen(self):
    try:
      while True:
        d = []
        data = self.db.video_interval.find(
            {'order': {'$exists': True, '$ne': []}}).hint("idx_order").limit(100)
        for each in data:
          d.append(each)
        data = self.db.video_interval.find(
            {'next': {'$lt': datetime.datetime.utcnow()}}).limit(100)
        for each in data:
          d.append(each)
        for data in d:
          # 如果存在手动操作，则刷新数据
          if 'order' in data:
            for order_id in data['order']:
              self.db.user_record.update_one({'_id': order_id}, {'$set': {
                  'isExecuted': True
              }})
          data['next'] = data['next'] + \
              datetime.timedelta(seconds=data['interval'])
          data['order'] = []
          self.db.video_interval.update_one(
              {'bvid': data['bvid']}, {'$set': data})
        for data in d:
          yield data
    except Exception as e:
      self.logger.exception(e)
