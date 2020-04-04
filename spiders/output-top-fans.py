from db import db
import datetime
f = open("data.csv", "w", encoding="utf-8-sig")
f.writelines("name,date,value\n")
# author_list = []
# d = datetime.datetime(2019, 1, 1)
# while d < datetime.datetime.now():
#   aggregate_result = db.author_data.aggregate(
#       [
#           {
#               "$match": {'datetime': {"$lt": d}}
#           },
#           {
#               "$group": {
#                   "_id": "$mid",
#                   "max": {"$max": "$fans"}
#               }
#           },
#           {
#               "$sort": {"max": -1}
#           },
#           {
#               "$limit": 20
#           }
#       ]
#   )
#   d += datetime.timedelta(30)
#   for each in aggregate_result:
pass

authors = db.author.find({}, {'mid': 1, 'name': 1}).sort([
    ('cFans', -1)]).limit(30)
for author in authors:
  mid = author['mid']
  author_data = db.author_data.find(
      {'mid': mid, "datetime": {"$gt": datetime.datetime(2020, 3, 20)}}).sort([('datetime', 1)])
  author_data = list(author_data)
  if author_data == None or len(author_data) < 1:
    continue
  p_val = author_data[0]['fans']
  p_dt = author_data[0]['datetime']
  l_val = author_data[-1]['fans']
  l_dt = author_data[-1]['datetime']
  c_time = datetime.datetime(
      p_dt.year, p_dt.month, p_dt.day)
  delta = datetime.timedelta(0.125)
  result = []
  c_time += delta
  for i in range(1, len(author_data)):
    n_dt = author_data[i]['datetime']
    n_val = author_data[i]['fans']
    while c_time < p_dt:
      c_time += delta
    while c_time < n_dt:
      delta_val = n_val - p_val
      delta_dt = n_dt.timestamp() - p_dt.timestamp()
      c_val = p_val + delta_val * \
          ((c_time.timestamp() - p_dt.timestamp())/delta_dt)

      result.append([c_time, c_val])
      c_time += delta
    p_val = n_val
    p_dt = n_dt
    pass
  for each_data in result:
    if each_data[0] > datetime.datetime(2019, 1, 1) and each_data[0] < datetime.datetime(2020, 4, 3, hour=17):
      f.writelines('"{}","{}","{}"\n'.format(
          author['name'], each_data[0], each_data[1]))
  pass
