from db import db
from utils import dec, enc
for each_video in db.video.find({'bvid': {'$exists': False}}, {'aid': 1, 'bvid': 1}).batch_size(20):
  bvid = enc(each_video['aid']).lstrip('BV')
  db.video.update_one({'aid': each_video['aid']}, {'$set': {
      'bvid': bvid
  }})
  print(each_video['aid'], bvid)
  pass
