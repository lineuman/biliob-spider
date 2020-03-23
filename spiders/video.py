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


class BiliobVideoSpider(Spider):
  def sentCallBack(self, object_id, coll):
    if object_id != None and object_id != 'null':
      coll.update_one({'_id': ObjectId(object_id)}, {
          '$set': {'isExecuted': True}})

  def gen_url(self):
    while True:
      url = get_url_from_redis("videoRedis:start_urls")
      yield url

  def parse(self, res):
    r = res.json()
    d = r["data"]
    keys = list(d.keys())
    for each_key in keys:
      if 'aid' in each_key:
        aid = d[each_key]['stat']['aid']
      else:
        aid = None
      bvid = d[each_key]['bvid'].lstrip('BV')
      author = d[each_key]['owner']['name']
      mid = d[each_key]['owner']['mid']
      view = d[each_key]['stat']['view']
      favorite = d[each_key]['stat']['favorite']
      danmaku = d[each_key]['stat']['danmaku']
      coin = d[each_key]['stat']['coin']
      share = d[each_key]['stat']['share']
      like = d[each_key]['stat']['like']
      reply = d[each_key]['stat']['reply']
      current_date = datetime.datetime.now()
      #  视频=硬币*0.4+收藏*0.3+弹幕*0.4+评论*0.4+播放*0.25+点赞*0.4+分享*0.6
      data = {
          'view': view,
          'favorite': favorite,
          'danmaku': danmaku,
          'coin': coin,
          'share': share,
          'like': like,
          'reply': reply,
          'jannchie': int(coin * 0.4 + favorite * 0.3 + danmaku * 0.4 + reply * 0.4 + view * 0.25 + like * 0.4 + share * 0.6),
          'datetime': current_date
      }

      subChannel = d[each_key]['tname']
      title = d[each_key]['title']
      date = d[each_key]['pubdate']
      tid = d[each_key]['tid']
      pic = d[each_key]['pic']
      item = {}
      item['current_view'] = view
      item['current_favorite'] = favorite
      item['current_danmaku'] = danmaku
      item['current_coin'] = coin
      item['current_share'] = share
      item['current_reply'] = reply
      item['current_like'] = like
      item['current_datetime'] = current_date
      item['current_jannchie'] = int(coin * 0.4 + favorite * 0.3 + danmaku *
                                     0.4 + reply * 0.4 + view * 0.25 + like * 0.4 + share * 0.6)
      item['aid'] = aid
      item['mid'] = mid
      item['pic'] = pic
      item['author'] = author
      item['bvid'] = bvid
      item['data'] = data
      item['title'] = title
      item['subChannel'] = subChannel
      item['datetime'] = date

      if subChannel != '':
        if subChannel not in sub_channel_2_channel:
          item['channel'] = ''
          self.logger.fatal(subChannel)
        else:
          item['channel'] = sub_channel_2_channel[subChannel]
      elif subChannel == '资讯':
        if tid == 51:
          item['channel'] == '番剧'
        if tid == 170:
          item['channel'] == '国创'
        if tid == 159:
          item['channel'] == '娱乐'
      else:
        item['channel'] = None

      url_list = res.url.split('&')
      if len(url_list) == 2:
        item['object_id'] = url_list[1]
      else:
        item['object_id'] = None
      return item

  def save(self, item):
    if db['aid'] == None:
      data_filter = {
          'bvid': item['bvid']
      }
    else:
      data_filter = {'aid': item['aid']}
    db['video'].update_one(data_filter, {
        '$set': {
            'cView': item['current_view'],
            'cFavorite': item['current_favorite'],
            'cDanmaku': item['current_danmaku'],
            'cCoin': item['current_coin'],
            'cShare': item['current_share'],
            'cLike': item['current_like'],
            'cReply': item['current_reply'],
            'cJannchie': item['current_jannchie'],
            'cDatetime': item['current_datetime'],
            'author': item['author'],
            'subChannel': item['subChannel'],
            'channel': item['channel'],
            'bvid': item['bvid'],
            'mid': item['mid'],
            'pic': item['pic'],
            'title': item['title'],
            'datetime': datetime.datetime.fromtimestamp(
                item['datetime'])
        },
        '$push': {
            'data': {
                '$each': [item['data']],
                '$position': 0
            }
        }
    }, True)
    return item


s = BiliobVideoSpider("旧视频爬虫")
sc = SimpyderConfig()
sc.USER_AGENT = FAKE_UA
s.set_config(sc)
if __name__ == "__main__":
  s.run()
