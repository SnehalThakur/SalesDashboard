from apscheduler.schedulers.background import BackgroundScheduler

from SchedulerTask import print_mongo

scheduler = BackgroundScheduler()

scheduler.add_job(print_mongo, 'interval', seconds=3)
