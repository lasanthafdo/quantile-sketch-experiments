import csv
import sdv
from sdv.tabular import GaussianCopula, CTGAN
from sdv.evaluation import evaluate
from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
from json import dumps
import pandas as pd
import datetime
import os
import sys

workload_type = "-"
MODEL_FILE = "-"
if "ysb" in sys.argv[1]:
    MODEL_FILE = 'ysb_gaussian_model.pkl'
    workload_type = "ysb"
elif "nyt" in sys.argv[1]:
    MODEL_FILE = 'nyt_gaussian_model.pkl'
    workload_type = "nyt"

model = GaussianCopula.load(MODEL_FILE)

# ysb
sampled_data = model.sample(1000)
view_events = 0
click_events = 0
purchase_events = 0
hm = {}

# nyt
fare_amount_sample = 0.0

total_events = 0
if workload_type == "ysb":
    for sampled_data_point in sampled_data.iterrows():
        total_events = total_events + 1
        if sampled_data_point[1]["event_type"] == "view":
            view_events = view_events + 1
        elif sampled_data_point[1]["event_type"] == "purchase":
            purchase_events = purchase_events + 1
        elif sampled_data_point[1]["event_type"] == "click":
            click_events = click_events + 1

        if sampled_data_point[1]["ad_id"] in hm.keys():
            hm[sampled_data_point[1]["ad_id"]] = hm[sampled_data_point[1]["ad_id"]] + 1
        else:
            hm[sampled_data_point[1]["ad_id"]] = 1
elif workload_type == "nyt":
    for sampled_data_point in sampled_data.iterrows():
        total_events = total_events + 1
        fare_amount_sample = fare_amount_sample + float(sampled_data_point[1]["fare_amount"])




#for k, v in hm.items():
    #print(str(k) + ": " + str(v))

print("total")
print(total_events)
print("view_events")
print(view_events)
print("click_events")
print(click_events)
print("purchase_events")
print(purchase_events)
print("------------------------")


# ysb
data_view_count = 0
data_total_count = 0
data_click_count = 0
data_purchase_count = 0


# nyt
avg_fare_data = 0.0
fare_total = 0.0

with open(sys.argv[1]) as csvfile:
    if workload_type == "ysb#":
        reader = csv.DictReader(csvfile, fieldnames=["ad_id", "event_type", "ad_type"])
        for event in reader:
            data_total_count = data_total_count + 1
            if event["event_type"] == "view":
                data_view_count = data_view_count + 1
            elif event["event_type"] == "purchase":
                data_purchase_count = data_purchase_count + 1
            elif event["event_type"] == "click":
                data_click_count = data_click_count + 1
        print("data_total_count")
        print(data_total_count)
        print("data_view_count")
        print(data_view_count)
        print("data_click_count")
        print(data_click_count)
        print("data_purchase_count")
        print(data_purchase_count)
    elif workload_type == "nyt":
        reader = csv.DictReader(csvfile) # fieldnames=["trip_time_in_secs", "trip_distance", "fare_amount"]
        for event in reader:
            data_total_count = data_total_count + 1
            fare_total = float(event["fare_amount"]) + fare_total
            avg_fare_data = fare_total/data_total_count
        print("avg_fare_real")
        print(avg_fare_data)
        print("avg_fare_sample")
        print(fare_amount_sample/total_events)



print('evaluate')
real_data = pd.read_csv(sys.argv[1])
print(evaluate(sampled_data, real_data, metrics=['CSTest', 'KSTest'], aggregate=False))


#with open('sampled_data_1.txt') as csvfile:
#    spamreader = csv.DictReader(csvfile)
#    view_events = 0
#    total_events = 0
#    for row in spamreader:
#        total_events = total_events + 1
#        if(row["event_type"] == "view"):
#            view_events = view_events + 1
#    print("total_events")
#    print(total_events)
#    print("view_events")
#    print(view_events)

