import schedule
import time
from datetime import datetime
from script import run_stock_job

def basic_job():
    print(f"Basic job executed at {datetime.now()}")
    
#run every minute
schedule.every(1).minutes.do(run_stock_job)
schedule.every().minutes.do(basic_job)
while True:
    schedule.run_pending()
    time.sleep(1)