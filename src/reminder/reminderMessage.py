import os, time

#Todo:Download this lib
from apscheduler.schedulers.background import BackgroundScheduler

from datetime import datetime, timedelta


class Reminder:

    scheduler=""

    def __init__(self):
        self.date=""
        self.scheduler = BackgroundScheduler()

    def tick(self,text):
        print(text + '! The time is: %s' % datetime.now())

    def reminderAt(self,year,month,day):

        dd = datetime(year,month,day) + timedelta(hours=10,minutes=0,seconds=0)
        print(dd)
        self.scheduler.add_job(self.tick, 'date', run_date=dd, args=['TICK'])
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



# reminder=Reminder();
# reminder.reminderAt()
