from utils import enc, dec
from biliob import BiliobSpider
from time import sleep
from datetime import datetime
from datetime import timedelta
url = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid={}&pagesize=10&page=1&order=pubdate'
url = 'https://api.bilibili.com/x/space/arc/search?mid={}&ps=100&tid=0&pn=1&keyword=&order=pubdate'


class AddPublicVideoSpider(BiliobSpider):
  def __init__(self):
    super().__init__(name='追加作者最新上传的视频')

  def gen_url(self):
    while True:
      try:
        cursor = self.db.author.find({}, {'mid': 1}).batch_size(20)
        try:
          for each_author in cursor:
            yield url.format(each_author['mid'])
        except Exception as e:
          self.logger.exception(e)
          sleep(1)
      except Exception as e:
        self.logger.exception(e)
        sleep(1)

  def parse(self, res):
    try:
      j = res.json()
      result = []
      if 'data' not in j or 'list' not in j['data'] or 'vlist' not in j['data']['list']:
        return None
      bvid = None
      aid = None
      for each_video in j['data']['list']['vlist']:
        if 'bvid' in each_video:
          bvid = each_video['bvid']
        if 'aid' in each_video:
          aid = each_video['aid']
        result.append([bvid, aid])
      return result
    except Exception as e:
      self.logger.exception(e)

  def update_video_interval(self, interval: int, aid, bvid):
    now = datetime.utcnow() + timedelta(hours=8)
    if aid == None:
      aid = enc(bvid)
    if bvid == None:
      bvid = dec(aid)
    return {
        'next': now,
        'interval': interval,
        'aid': aid,
        'bvid': bvid
    }

  def save(self, items):
    for [bvid, aid] in items:
      bvid = bvid.lstrip('BV')
      interval_data = self.db.video_interval.find_one(
          {'bvid': bvid, 'aid': aid})
      if interval_data == None:
        self.db.video_interval.update_one(
            {'bvid': bvid, 'aid': aid}, {'$set': self.update_video_interval(3600 * 12, aid, bvid), '$setOnInsert': {'date': datetime.utcnow() + timedelta(hours=8)}}, upsert=True)

    return items


s = AddPublicVideoSpider()


if __name__ == "__main__":

  s.run()
