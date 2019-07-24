import os, time

#Todo:Download this lib
from apscheduler.schedulers.background import BackgroundScheduler

from datetime import datetime, timedelta

from db_subscriber.db_subscriber import DBSubscriber

class Reminder:

    scheduler=""

    def __init__(self):
        self.date=""
        self.scheduler = BackgroundScheduler()

    def tick(self,q_name,msg):
        # print(text)
        DBSubscriber._sender.send_msg(q_name, msg)

    def reminderAt(self,year,month,day,q_name,msg):

        dd = datetime(year,month,day) + timedelta(hours=10,minutes=0,seconds=0)
        # print(dd)
        self.scheduler.add_job(self.tick, 'date', run_date=dd, args=[q_name,msg])
        self.scheduler.start()

        # print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
        # dd = datetime.now() + timedelta(seconds=6)
        # scheduler.add_job(self.tick(), 'date', run_date=dd, kwargs={'text': 'TOCK'})

        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            # Not strictly necessary if daemonic mode is enabled but should be done if possible
            self.scheduler.shutdown()
            DBSubscriber.reminderThread.join()



# reminder=Reminder();
# reminder.reminderAt(2019,7,24)
