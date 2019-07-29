from db_subscriber.db_subscriber import DBSubscriber
import argparse
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from utils.logger import Logger

#for timing
import time

#Todo:Download this lib
from apscheduler.schedulers.background import BackgroundScheduler

from datetime import datetime, timedelta

from db_subscriber.db_subscriber import DBSubscriber
import threading
import cx_Oracle
#****************

DESCRIPTION = "Utility for oracle table subscription"

#TODO:Schedual
oldThread=""
scheduler=""

def send():

    scheduler.remove_all_jobs()

    db_sub.subscribeReminer()
    sql_ret = db_sub.db.cursor().callfunc("schedual_NOTIFICATION", cx_Oracle.CURSOR,[alarmTime])

    db_sub.send_sql_cur(sql_ret)

    #todo:Reminder old terminated
    # oldThread=threading.current_thread()

    # todo:Reminder Every day
    flag = False
    schedual(flag)

def schedual(flag):

    dateNow = datetime.now().today()

    if(flag):

        if (dateNow.hour < 10):

            alarmTime = dateNow.replace(hour=10, minute=0, second=0)

        else:

            alarmTime = (dateNow + timedelta(days=1)).replace(hour=10, minute=0, second=0)
    else:

        alarmTime = (dateNow + timedelta(days=1)).replace(hour=10, minute=0, second=0)

    scheduler.add_job(send, 'date', run_date=alarmTime)
    if(flag):
        scheduler.start()

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)

    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
        # DBSubscriber.reminderThread.join()


if __name__ == "__main__":

    #todo:schdule
    flag=True

    alarmTime=""

    scheduler = BackgroundScheduler()

    parser = argparse.ArgumentParser(description='DESCRIPTION')
    parser.add_argument('-c', '--cfg', help='JSON configuration file', required=False, default="settings.json")
    args = vars(parser.parse_args())

    cfg_file = args["cfg"]
    db_sub = DBSubscriber(json_config_dir=cfg_file)

    try:
        db_sub.subscribe()

        #todo:Reminder Every day
        th=threading.Thread(target=schedual,args=[flag])
        th.start()

        input("Press any key to kill the main thread\n")
    except Exception as e:
        Logger.log.exception(e)



