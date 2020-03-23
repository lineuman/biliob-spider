from biliob import BiliobSpider
from time import sleep
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

  def save(self, items):
    for aid in items:
      self.db.video.update_one({'aid': aid}, {'$setOnInsert': {'aid': aid}})
    return items


s = AddPublicVideoSpider()


if __name__ == "__main__":

  s.run()
