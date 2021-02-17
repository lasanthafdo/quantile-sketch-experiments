import sdv
import time
from sdv.tabular import GaussianCopula, CTGAN
from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
from json import dumps
import pandas as pd
import random
import datetime
import os
from queue import LifoQueue
import multiprocessing as mp
import socket


# Writing this comment
dir_path = os.path.dirname(os.path.realpath(__file__))
print("dir_path: " + dir_path)

KAFKA_SEND_TOPIC = "ad-events-1"
KAFKA_RECEIVE_TOPIC = "stragglers"
#KAFKA_RECEIVE_TOPIC = "ad-events-1"

GAUSSIAN_MODEL_FILE = 'gaussian_model.pkl'
GAN_MODEL_FILE = 'gan_model.pkl'
SAMPLE_AMOUNT = 800

kafka_port = 9092
if socket.gethostname() == "Harshs-MBP":
    kafka_broker = "localhost"
else:
    kafka_broker = "tem101.tembo-domain.cs.uwaterloo.ca"

server = kafka_broker + ":" + str(kafka_port)
bootstrap_servers = [server]

consumer_regular = KafkaConsumer(
    KAFKA_RECEIVE_TOPIC,
    bootstrap_servers=server,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-' + str(random.randint(0, 10)),
    value_deserializer=lambda x: loads(x.decode('utf-8')))

producer_regular = KafkaProducer(
    bootstrap_servers=server,
    key_serializer=lambda x: (x.encode('utf-8')),
    value_serializer=lambda x: (x.encode('utf-8')))

HEADERS = ["ad_id", "ad_type", "event_type"]

def create_fake_data_model(df, lock):
    print("Created New Process")
    start_time = datetime.datetime.now()
    # create a new model here
    #df.to_csv("temporary.txt", index=False)
    #df = pd.read_csv("temporary.txt")
    ctgan_model = GaussianCopula(field_transformers={
        'ad_id': 'label_encoding'
    })
    print("Fitting Model")
    ctgan_model.fit(df)

    print("Acquiring Lock to save")
    lock.acquire()
    print("Lock Acquired")
    ctgan_model.save(dir_path + "/" + GAUSSIAN_MODEL_FILE)
    print("File Saved")
    lock.release()
    print("Lock Released")

    end_time = datetime.datetime.now()
    time_diff = end_time - start_time
    print("End Training Model")
    execution_time = round(time_diff.seconds, 4)
    print("Training took " + str(execution_time) + " seconds")

def sample_data(num_to_sample, last_watermark):
    sdv_instance = sdv.SDV()
    lock.acquire()
    model = sdv_instance.load(GAUSSIAN_MODEL_FILE)
    lock.release()
    sampled_data = model.sample(int(num_to_sample))
    for sampled_data_point in sampled_data.iterrows():
        new_data_point = {}
        new_data_point["user_id"] = "-"
        new_data_point["page_id"] = "-"
        new_data_point["ad_id"] = sampled_data_point[1]["ad_id"]
        new_data_point["ad_type"] = sampled_data_point[1]["ad_type"]
        new_data_point["event_type"] = sampled_data_point[1]["event_type"]
        new_data_point["event_time"] = last_watermark
        new_data_point["ip_address"] = "-"
        new_data_point["gen_time"] = current_milli_time();
        producer_regular.send(KAFKA_SEND_TOPIC, dumps(new_data_point), dumps(new_data_point))

def current_milli_time():
    return round(time.time() * 1000)

if __name__ == "__main__":
    mp.set_start_method('forkserver')
    lock = mp.Lock()
    tmp_data = []
    curr_data_points = 0
    last_model_start_time = current_milli_time() - 1000000
    print("Starting Server")
    new_models_created = 0
    for msg in consumer_regular:
        event = msg.value
        num_to_sample = event.get("EventsDiscardedSinceLastWatermark")
        if (num_to_sample is None):  # is straggler event
            curr_data_points = curr_data_points + 1
            tmp_dict = {}
            tmp_dict.update({"ad_id" : event["ad_id"]})
            tmp_dict.update({"event_type" : event["event_type"]})
            tmp_dict.update({"ad_type" : event["ad_type"]})
            tmp_data.append(tmp_dict)
            if len(tmp_data) > 11000:
                tmp_data.pop()
        else:  # is watermark event
            if os.path.exists(dir_path + "/" + GAUSSIAN_MODEL_FILE) and num_to_sample > 0:
                sdv_instance = sdv.SDV()
                lock.acquire()
                model = sdv_instance.load(GAUSSIAN_MODEL_FILE)
                lock.release()
                sampled_data = model.sample(int(num_to_sample))
                last_watermark = event["lastWatermark"]
                print("Sending sample: " + str(num_to_sample) + " last_watermark: " + str(last_watermark)
                      + " current_time: " + str(current_milli_time()))
                for sampled_data_point in sampled_data.iterrows():
                    new_data_point = {}
                    new_data_point["user_id"] = "-"
                    new_data_point["page_id"] = "-"
                    new_data_point["ad_id"] = sampled_data_point[1]["ad_id"]
                    new_data_point["ad_type"] = sampled_data_point[1]["ad_type"]
                    new_data_point["event_type"] = sampled_data_point[1]["event_type"]
                    new_data_point["event_time"] = last_watermark
                    new_data_point["ip_address"] = "-"
                    new_data_point["fake"] = "true"
                    producer_regular.send(KAFKA_SEND_TOPIC, dumps(new_data_point), dumps(new_data_point))

        if curr_data_points > 10000 and (current_milli_time() - last_model_start_time) > 15000:
            print("starting new thread creating new model")
            last_model_start_time = current_milli_time()
            df = pd.DataFrame(tmp_data, columns=["ad_id", "event_type", "ad_type"])
            #df.to_csv("temp" + str(new_models_created) + ".csv", index=False)
            new_models_created = new_models_created + 1
            process = mp.Process(target=create_fake_data_model, args=(df, lock,))
            process.start()
            curr_data_points = 0
            tmp_data = []
