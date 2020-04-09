from time import sleep
from utils import sub_channel_2_channel
from new_video import BiliobNewVideoSpider
import datetime
url = 'https://api.bilibili.com/x/article/archives?ids={}'
# 该爬虫依赖于AV号


class Spider(BiliobNewVideoSpider):
  def __init__(self):
    super().__init__("超级视频爬虫", thread=8, interval=0.1)

  def gen_url(self):
    gen = self.video_gen_without_lock()
    aid_list = []
    for each_video in gen:
      if len(aid_list) > 80:
        yield url.format(','.join(str(x) for x in aid_list))
        aid_list = []
      if each_video != None:
        aid_list.append(each_video['aid'])
      else:
        sleep(5)

  def parse(self, res):
    r = res.json()
    datas = r["data"]
    result = []
    for each_aid in datas:
      d = datas[each_aid]
      if 'aid' in d:
        aid = d['stat']['aid']
      else:
        aid = None
      bvid = d['bvid'].lstrip('BV')
      author = d['owner']['name']
      mid = d['owner']['mid']
      view = d['stat']['view']
      favorite = d['stat']['favorite']
      danmaku = d['stat']['danmaku']
      coin = d['stat']['coin']
      share = d['stat']['share']
      like = d['stat']['like']
      reply = d['stat']['reply']
      current_date = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
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

      subChannel = d['tname']
      title = d['title']
      date = d['pubdate']
      tid = d['tid']
      pic = d['pic']
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
      result.append(item)
    return result

  def save(self, items):
    for item in items:
      if self.db['aid'] != None:
        data_filter = {'aid': item['aid']}
      else:
        data_filter = {'bvid': item['bvid']}
      self.db['video'].update_one(data_filter, {
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


s = Spider()
if __name__ == "__main__":
  s.run()
