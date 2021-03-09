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


if "Harshs" in socket.gethostname():
    dir_path = os.path.dirname(os.path.realpath(__file__))
else:
    dir_path = "/hdd2/sdv"

print("dir_path: " + dir_path)

HEADERS = ["ad_id", "ad_type", "event_type"]

#locks
DATA_FILE = dir_path + "/" + sys.argv[1]
LOCK_PATH_DATA = DATA_FILE + ".lock"
lock_data = FileLock(LOCK_PATH_DATA)


workload_type = "-"
MODEL_FILE = "-"
if "ysb" in sys.argv[1]:
    workload_type = "ysb"
    MODEL_FILE = dir_path + "/" + 'ysb_gaussian_model.pkl'
elif "nyt" in sys.argv[1]:
    workload_type = "nyt"
    MODEL_FILE = dir_path + "/" + 'nyt_gaussian_model.pkl'
else:
    print("workload type unknown")
    exit()

LOCK_PATH_MODEL = MODEL_FILE + ".lock"
lock_model = FileLock(LOCK_PATH_MODEL)


def create_fake_data_model(df, workload_type):
    # create a new model here
    if workload_type == "ysb":
        ctgan_model = GaussianCopula(field_transformers={
            'ad_id': 'label_encoding',
            "event_type": "label_encoding",
            "ad_type": "label_encoding",
        })
        # ctgan_model = GaussianCopula(field_transformers={
        #    'ad_id': 'categorical',
        #    "event_type": "categorical",
        #    "ad_type": "categorical",
        # })
    elif workload_type == "nyt":
        ctgan_model = GaussianCopula(field_transformers={
            'fare_amount': 'float',
            "trip_distance": "float",
            "trip_time_in_secs": "integer",
        })

    print("Fitting Model")
    start_time = datetime.datetime.now()
    ctgan_model.fit(df)

    print("Acquiring Lock to save")
    lock_model.acquire()
    print("Lock Acquired")
    ctgan_model.save(MODEL_FILE)
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
        if (current_milli_time() - last_model_start_time) > 10000:
            print("Checking for " + DATA_FILE)
            if os.path.exists(DATA_FILE):
                lock_data.acquire()
                with open(DATA_FILE, 'r+') as csvfile:
                    sha1 = hashlib.sha1()
                    reader = csv.DictReader(csvfile)
                    for event in reader:
                        if workload_type == "ysb":
                            curr_data_points = curr_data_points + 1
                            sha1.update(event["ad_id"].encode())
                            tmp_dict = {}
                            tmp_dict.update({"ad_id": event["ad_id"]})
                            tmp_dict.update({"event_type": event["event_type"]})
                            tmp_dict.update({"ad_type": event["ad_type"]})
                            tmp_data.append(tmp_dict)
                        elif workload_type == "nyt":
                            curr_data_points = curr_data_points + 1
                            sha1.update(str(event["trip_time_in_secs"]).encode())
                            tmp_dict = {}
                            tmp_dict.update({"fare_amount": float(event["fare_amount"])})
                            tmp_dict.update({"trip_time_in_secs": int(event["trip_time_in_secs"])})
                            tmp_dict.update({"trip_distance": float(event["trip_distance"])})
                            tmp_data.append(tmp_dict)
                lock_data.release()

                if sha1.hexdigest() != last_sha:
                    last_sha = sha1.hexdigest()
                    last_model_start_time = current_milli_time()
                    if workload_type == "ysb":
                        df = pd.DataFrame(tmp_data, columns=["ad_id", "event_type", "ad_type"])
                    elif workload_type == "nyt":
                        df = pd.DataFrame(tmp_data, columns=["fare_amount", "trip_time_in_secs", "trip_distance"])
                    new_models_created = new_models_created + 1
                    create_fake_data_model(df, workload_type)
                    curr_data_points = 0
                else:
                    print("data file has not been updated")
                    time.sleep(3)
            else:
                print("data file has not been created")
                time.sleep(3)
        else:
            sleep_time = int((10000 - (current_milli_time() - last_model_start_time)) / 1000) + 1
            print("sleeping for " + str(sleep_time) + " seconds")
            time.sleep(sleep_time)
