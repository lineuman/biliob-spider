from db import db
import datetime


def get_data(mid):
  last_doc = db.author_daily_trend.find_one(
      {'mid': mid}, sort=[['datetime', -1]])
  if last_doc == None:
    date = '自古'
    c = db.author_data.find({'mid': mid}).sort([['datetime', 1]])
  else:
    date = last_doc['datetime']
    delta = datetime.datetime.now().timestamp() - date.timestamp() - 86400 * 2
    if delta < 0:
      return []
    c = db.author_data.find(
        {'mid': mid, 'datetime': {'$gt': last_doc['datetime']}}).sort([['datetime', 1]])
  return list(c)


def standardization(data):
  start_datetime = data[0]['datetime']
  end_datetime = data[-1]['datetime']
  # 检查点
  cd = datetime.datetime(
      start_datetime.year, start_datetime.month, start_datetime.day)
  out = []
  # 前驱时间
  p_data = data[0]
  for key in ['archiveView', 'fans', 'like']:
    p_data.setdefault(key)
  p_dt = p_data['datetime']
  for i in range(1, len(data)):
    # 后驱时间
    n_data = data[i]
    n_dt = n_data['datetime']
    while cd < p_dt:
      # 如果检查点在前驱前，则后移检查点
      cd += datetime.timedelta(1)
    if n_dt < cd:
      # 如果检查点在后驱后，则后移窗口
      continue
    # 获取值
    result = {'datetime': cd}
    for key in ['archiveView', 'fans', 'like']:
      p_val = p_data[key]
      n_val = None
      if key in n_data:
        n_val = n_data[key]
      if p_val == None or n_val == None:
        result[key] = None
        continue
      # 计算差值数据
      delta_v = n_val - p_val
      start_v = p_val
      delta_t = n_dt.timestamp() - p_dt.timestamp()
      delta_c = cd.timestamp() - p_dt.timestamp()
      cv = delta_v / delta_t * delta_c + start_v
      result[key] = cv
    # cd += datetime.timedelta(1)
    out.append(result)
    for key in ['archiveView', 'fans', 'like']:
      if key in n_data and n_data[key] != None:
        p_data[key] = n_data[key]
    p_dt = n_dt
  return out


def get_daily_trend(data):
  result = []
  for i in range(1, len(data)):
    c_data = data[i]
    c_date = data[i]['datetime']
    p_data = data[i - 1]
    p_date = data[i - 1]['datetime']
    c_result = {'datetime': p_date}
    for key in ['archiveView', 'fans', 'like']:
      c_value = c_data[key]
      p_value = p_data[key]
      if c_value in [None, 0] or p_value in [None, 0]:
        continue
      d_value = int(c_value - p_value)
      c_result[key] = d_value
      pass
    result.append(c_result)
  return result


def add_data(mid, data):
  for d in data:
    d['mid'] = mid
    db.author_daily_trend.replace_one(
        {'mid': mid, 'datetime': d['datetime']}, d, upsert=True)
  pass


for each_author in db.author.find({}, {'mid': 1}).batch_size(100):
  mid = each_author['mid']
  data = get_data(mid)
  if data == None or data == []:
    print(mid, '数据已更新到最新')
    continue
  print('正在计算', mid, data[0]['datetime'], '以来的数据')
  # datetime 为北京时间
  if len(data) <= 1:
    continue
  standard_data = standardization(data)
  daily_trend = get_daily_trend(standard_data)
  add_data(mid, daily_trend)
  pass
