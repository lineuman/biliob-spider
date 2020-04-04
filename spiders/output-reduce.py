from db import db
import datetime
from csv import DictReader, DictWriter

d = DictReader(open("data.csv", "r", encoding="utf-8-sig"))
pass
d = list(d)

d.sort(key=lambda e: float(e['value']), reverse=True)
count = {}
result = []
for each in d:

  if each['date'] not in count:
    count[each['date']] = 0
  if count[each['date']] == 20:
    continue
  result.append(each)
  count[each['date']] += 1
pass
f = open("o.csv", "w", encoding="utf-8-sig")
f.writelines("name,date,value\n")
result.sort(key=lambda x: x['date'])
for e in result:
  f.writelines('"{}","{}","{}"\n'.format(e['name'], e['date'], e['value']))
