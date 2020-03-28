from db import db
from datetime import datetime
_id = db.author.find_one({'mid': 1841196}, {'_id': 1})['_id']
for each_author in db.author.find({'_id': {'$gt': _id}}).batch_size(20):
  if 'data' not in each_author:
    continue
  data = each_author['data']
  for each_data in data:
    each_data['mid'] = each_author['mid']
    db.author_data.replace_one(
        {'mid': each_data['mid'], 'datetime': each_data['datetime']}, each_data, upsert=True)
  print(datetime.now(), each_author['mid'])
