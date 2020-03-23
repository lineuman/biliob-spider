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

  def get_new_author_from_interval(self):
    try:
      # 先检查有没有手动操作
      data = self.db.author_interval.find_one({'order.0': {'$exists': 1}})
      if data == None:
        data = self.db.author_interval.find_one(
            {'next': {'$lt': datetime.datetime.now()}})
      else:
        # 如果存在手动操作，则刷新数据
        for order_id in data['order']:
          self.db.user_record.update({'_id': order_id}, {'$set': {
              'isExecuted': True
          }})
      if data != None:
        data['next'] = data['next'] + \
            datetime.timedelta(seconds=data['interval'])
        data['order'] = []
        self.db.author_interval.update({'mid': data['mid']}, {'$set': data})
      return data
    except Exception as e:
      self.logger.exception(e)

  def get_new_video_from_interval(self):
    try:
      # 先检查有没有手动操作
      data = self.db.video_interval.find_one({'order.0': {'$exists': 1}})
      if data == None:
        data = self.db.video_interval.find_one(
            {'next': {'$lt': datetime.datetime.now()}})
      else:
        # 如果存在手动操作，则刷新数据
        for order_id in data['order']:
          self.db.user_record.update({'_id': order_id}, {'$set': {
              'isExecuted': True
          }})
      if data != None:
        data['next'] = data['next'] + \
            datetime.timedelta(seconds=data['interval'])
        data['order'] = []
        self.db.video_interval.update({'bvid': data['bvid']}, {'$set': data})
      return data
    except Exception as e:
      self.logger.exception(e)
