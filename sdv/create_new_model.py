import time
from sdv.tabular import GaussianCopula, CTGAN
from json import loads, dumps
import pandas as pd
import random
import datetime
from filelock import Timeout, FileLock
import hashlib
import sys
import csv
import os
import socket


if socket.gethostname() == "Harshs-MBP":
    dir_path = os.path.dirname(os.path.realpath(__file__))
else:
    dir_path = "/hdd2/sdv"
print("dir_path: " + dir_path)

GAUSSIAN_MODEL_FILE = dir_path + '/' + 'gaussian_model.pkl'
SAMPLE_AMOUNT = 800

HEADERS = ["ad_id", "ad_type", "event_type"]

#locks
DATA_FILE = dir_path + "/" + sys.argv[1]
LOCK_PATH_DATA = DATA_FILE + ".lock"

GAUSSIAN_MODEL_FILE = dir_path + "/" + 'gaussian_model.pkl'
LOCK_PATH_MODEL = GAUSSIAN_MODEL_FILE + ".lock"

lock_data = FileLock(LOCK_PATH_DATA)
lock_model = FileLock(LOCK_PATH_MODEL)


def create_fake_data_model(df):
    # create a new model here
    ctgan_model = GaussianCopula(field_transformers={
        'ad_id': 'label_encoding'
    })
    print("Fitting Model")
    start_time = datetime.datetime.now()
    ctgan_model.fit(df)

    print("Acquiring Lock to save")
    lock_model.acquire()
    print("Lock Acquired")
    ctgan_model.save(GAUSSIAN_MODEL_FILE)
    print("Lock Released")
    lock_model.release()

    end_time = datetime.datetime.now()
    time_diff = end_time - start_time
    execution_time = round(time_diff.seconds, 4)
    print("Training took " + str(execution_time) + " seconds")

def current_milli_time():
    return round(time.time() * 1000)


if __name__ == "__main__":
    last_model_start_time = current_milli_time() - 1000000
    tmp_data = []

    last_sha = ""
    new_models_created = 0
    curr_data_points = 0
    while True:
        if (current_milli_time() - last_model_start_time) > 15000:
            last_model_start_time = current_milli_time()

            if os.path.exists(DATA_FILE):
                lock_data.acquire()
                with open(DATA_FILE, 'r+') as csvfile:
                    sha1 = hashlib.sha1()
                    reader = csv.DictReader(csvfile)
                    for event in reader:
                        curr_data_points = curr_data_points + 1
                        sha1.update(event["ad_id"].encode())
                        tmp_dict = {}
                        tmp_dict.update({"ad_id": event["ad_id"]})
                        tmp_dict.update({"event_type": event["event_type"]})
                        tmp_dict.update({"ad_type": event["ad_type"]})
                        tmp_data.append(tmp_dict)
                    print(sha1.hexdigest())
                lock_data.release()

                if sha1.hexdigest() != last_sha:
                    last_sha = sha1.hexdigest()
                    last_model_start_time = current_milli_time()
                    df = pd.DataFrame(tmp_data, columns=["ad_id", "event_type", "ad_type"])
                    new_models_created = new_models_created + 1
                    create_fake_data_model(df)
                    curr_data_points = 0
                    time.sleep(15)
                else:
                    print("data file has not been updated")
                    time.sleep(5)
        else:
            sleep_time = int((15000 - (current_milli_time() - last_model_start_time)) / 1000) + 1
            print("sleeping for " + str(sleep_time) + " seconds")
            time.sleep(sleep_time)
