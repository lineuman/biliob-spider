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

daily_spiders = ['utils/keyword_author.py']


def check():
  for each_spider_group in [spiders, weekly_spider]:

    for each_spider in each_spider_group:
      pid = find_procs_by_name(each_spider)
      if len(pid) == 0:
        run_spider(each_spider)
    pass


def run_spider(spider):
  print('[{}] 重启 {}'.format(datetime.datetime.utcnow() +
                            datetime.timedelta(hours=8), spider))
  delete_by_name(spider)
  cmd = 'nohup python {} 1>{}.log 2>&1 &'.format(spider, spider)
  os.system(cmd)
  pass


schedule.every(10).seconds.do(check)
schedule.every().hour.do(run_spider, 'site_info.py')
schedule.every().day.at('09:00').do(run_spider, 'rank_add.py')
schedule.every().day.at('02:00').do(run_spider, 'calculate_author_daily.py')
schedule.every().day.at('03:00').do(run_spider, 'utils/keyword_author.py')
schedule.every().wednesday.at('03:20').do(run_spider, 'utils/keyword.py')
for each_spider in spiders:
  run_spider(each_spider)
while True:
  schedule.run_pending()
  sleep(10)
