from db import db
import datetime
import logging
video_coll = db['video']
videos = video_coll.find({})
for each_video in videos:
  try:
    if 'aid' not in each_video:
      continue

    if 'data' not in each_video or len(each_video['data']) < 100:
      continue
    print(each_video['aid'])
    aid = each_video['aid']
    data = sorted(each_video['data'],
                  key=lambda x: x['datetime'], reverse=True)
    c_data = data[0]
    c_date = data[0]['datetime'].strftime('%Y-%m-%d %H')
    f_data = [c_data]
    for each_data in data:
      delta_day = (datetime.datetime.utcnow() + datetime.timedelta(hours=8) -
                   each_data['datetime']).days
      if delta_day > 7:
        n_date = each_data['datetime'].strftime('%Y-%m-%d %H')
        # 如果不是同一小时
        if n_date != c_date:
          f_data.append(each_data)
          c_date = n_date
          pass
        pass
      else:
        f_data.append(each_data)
    video_coll.update_one({'aid': aid}, {'$set': {'data': f_data}})
    pass
  except Exception as e:
    logging.exception(e)
pass
