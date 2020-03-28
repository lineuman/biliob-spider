from db import db
from datetime import datetime
for each_author in db.author.find({}).batch_size(20):
  data = each_author['data']
  for each_data in data:
    each_data['mid'] = each_author['mid']
    db.author_data.replace_one(
        {'mid': each_data['mid'], 'datetime': each_data['datetime']}, each_data, upsert=True)
    print(datetime.now(), data)
