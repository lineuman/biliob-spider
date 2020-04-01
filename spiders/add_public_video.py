from utils import enc
from biliob import BiliobSpider
from time import sleep
from datetime import datetime
from datetime import timedelta
url = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid={}&pagesize=10&page=1&order=pubdate'


class AddPublicVideoSpider(BiliobSpider):
  def __init__(self):
    super().__init__(name='追加作者最新上传的视频', interval=0.5)

  def gen_url(self):
    while True:
      try:
        cursor = self.db.author.find({}, {'mid': 1}).batch_size(20)
        for each_author in cursor:
          yield url.format(each_author['mid'])
      except Exception as e:
        self.logger.exception(e)
        sleep(1)

  def parse(self, res):
    try:
      j = res.json()
      aid_list = []
      if 'data' not in j or 'vlist' not in j['data']:
        return None
      for each_video in j['data']['vlist']:
        aid_list.append(each_video['aid'])
      return aid_list
    except Exception as e:
      self.logger(e)

  def update_video_interval(self, interval: int, aid):
    now = datetime.utcnow() + timedelta(hours=8)
    return {
        'next': now,
        'date': now,
        'interval': interval,
        'aid': aid,
        'bvid': enc(aid)[2:]
    }

  def save(self, items):
    for aid in items:
      self.db.video_interval.update(
          {'aid': aid}, self.update_video_interval(3600, aid), upsert=True)

    return items


s = AddPublicVideoSpider()


if __name__ == "__main__":

  s.run()
