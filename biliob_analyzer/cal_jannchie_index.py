from db import db
import datetime
from time import sleep
import logging
weight = {'coin': 0.4,
          'favorite': 0.3,
          'danmaku': 0.4,
          'reply': 0.4,
          'view': 0.25,
          'like': 0.4,
          'share': 0.6}
sum_weight = sum(weight.values())
count = 0
for video in db['video'].find().batch_size(100):
  try:
    sleep(0.02)
    count += 1
    if count % 10000 == 0:
      date = datetime.datetime.utcnow() + datetime.timedelta(hours=8).strftime("%Y-%m-%d %H:%M")
      print("[{}] 计算完毕数量：{}".format(date, count))
    if 'data' not in video:
      continue
    for each_data in video['data']:
      if 'jannchie' not in each_data:
        jannchie = 0
        temp_weight = 0
        for each_key in weight:
          if each_key in each_data and each_data[each_key] != None:
            jannchie += weight[each_key] * each_data[each_key]
            temp_weight += weight[each_key]
        if temp_weight < sum_weight:
          jannchie = jannchie / temp_weight * sum_weight
        each_data['jannchie'] = int(jannchie)
    db['video'].save(video)
  except Exception as e:
    logging.exception(e)
  pass
