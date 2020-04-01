import datetime
from db import db
from simpyder import Spider, FAKE_UA, SimpyderConfig


class SiteInfoSpider(Spider):
  def gen_url(self):
    yield 'https://api.bilibili.com/x/web-interface/online'

  def parse(self, res):
    return res.json()['data']

  def save(self, item):
    item['datetime'] = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    db.site_info.insert_one(item)
    return item


if __name__ == "__main__":
  s = SiteInfoSpider("site-info")
  sc = SimpyderConfig()
  sc.PARSE_THREAD_NUMER = 1
  sc.DOWNLOAD_INTERVAL = 10
  sc.LOG_LEVEL = "DEBUG"
  sc.USER_AGENT = FAKE_UA
  sc.COOKIE = ''
  s.set_config(sc)
  s.run()
