from time import sleep
import datetime
import schedule
import psutil
import os


def find_procs_by_name(name):
  "Return a list of processes matching 'name'."
  ls = []

  for process in psutil.process_iter():
    try:
      for each in process.cmdline():
        if name in each:
          ls.append(process.pid)
          print(each)
          break
        pass
    except Exception as e:
      pass
  return ls


def delete_by_name(name):
  pids = find_procs_by_name(name)
  for pid in pids:
    os.kill(pid, 9)


spiders = ['add_public_video.py',
           'author_follow.py',
           'author.py',
           'video.py',
           'new_author.py',
           'new_video.py',
           'tag.py']

weekly_spider = [
    'utils/keyword.py'
]

daily_spiders = ['rank_add.py', 'utils/keyword_author.py']


def check():
  print('[{}] '.format(datetime.datetime.now()))
  for each_spider_group in [spiders, weekly_spider, daily_spiders]:

    for each_spider in spiders:
      pid = find_procs_by_name(each_spider)
      if len(pid) == 0:
        run_spider(each_spider)
    pass


def run_spider(spider):
  delete_by_name(spider)
  cmd = 'nohup python3 {} 1>{}.log 2>&1 &'.format(spider, spider)
  os.system(cmd)
  print(cmd)
  pass


schedule.every(10).seconds.do(check)
schedule.every().day.at('09:00').do(run_spider, 'rank_add.py')
schedule.every().day.at('03:00').do(run_spider, 'utils/keyword_author.py')
schedule.every().wednesday.at('03:20').do(run_spider, 'utils/keyword.py')

while True:
  schedule.run_pending()
  sleep(10)
