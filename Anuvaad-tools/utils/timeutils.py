import time
from datetime import datetime
import pendulum
import csv


ist = pendulum.timezone('Asia/Calcutta')


def get_current_time():
    s_time = int(round(time.time() * 1000))
    return s_time


def get_date_time():
    date_time = datetime.now(ist)
    return str(date_time)