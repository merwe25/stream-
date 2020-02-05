from __future__ import print_function
import pandas as pd
import time
import io
import csv
from time import sleep

def generate_log_line():
    for chunk in pd.read_csv('train.csv', chunksize=30):


        time.sleep(1)
        print(chunk)


