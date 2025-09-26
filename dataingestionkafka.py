import os
import json
import requests
import math
import time
import threading
import schedule
import sys
import signal
from datetime import datetime
from kafka import KafkaProducer
from hdfs import InsecureClient

API_KEYS=[
    "ciU1gpaf6bkNI10gBPDbY5LU4dlMMGSW",
    "FlZnbO9lB65KFaABqORXhd0Ri94MZazZ",
    "hSnICF4l1XNFbg9AMWapAJQU0Qhe1aiD",
    "6oV3YgopwRtfRyXufczW4YLDYYDNEqbx",
    "Q8LHiyAenCMezikDOvngZQHXVQlNSvwu",
    "1hzeLb5CbsaNRczkiA4PayIghbYz7nf7",
    "h8T2Esv7GahGJ30GHfCc1eFz67PwDE0L",
    "aUFPoNEh2wIjamtM1stXhpSmoDw87nmQ",
    "27pl4k7x7orbgwZTVZTi8kgBSfR8zoMD",
]

KAFKA_BROKER="localhost:9092"
TOPIC_TIER1="traffic_tier1"
TOPIC_TIER2_BATCH="traffic_tier2_batch"
TOPIC_TIER2_SNAPSHOT="traffic_tier2_snapshot"

TIER1_POINTS=[
    (28.5921, 77.1555),  # NH-48 Dhaula Kuan – Airport
    (28.7360, 77.1482),  # NH-44 Azadpur – Singhu
    (28.6465, 77.3150),  # NH-9 Anand Vihar – Ghaziabad
    (28.6926, 77.1541),  # Outer Ring Rd Punjabi Bagh – Mukarba
    (28.5672, 77.2100),  # Outer Ring Rd AIIMS – Ashram
    (28.6412, 77.1223),  # Ring Rd Rajouri Garden – Naraina
    (28.5671, 77.2523),  # Ring Rd Ashram – Sarai Kale Khan
    (28.6289, 77.2421),  # Ring Rd ITO – India Gate
    (28.5679, 77.2833),  # DND Flyway Delhi – Noida
    (28.5820, 77.2381),  # Barapullah Elevated Rd
    (28.5904, 77.2776),  # Delhi–Meerut Exp Nizamuddin – Akshardham
    (28.5014, 77.3001),  # Mathura Rd Badarpur – Ashram
    (28.6330, 77.2907),  # NH-24/NH-9 Akshardham – Ghaziabad
    (28.6710, 77.0605),  # Rohtak Rd Tikri – Punjabi Bagh
    (28.4657, 77.0853),  # MG Rd (Mehrauli – Gurgaon)
    (28.4975, 77.3057),  # NH-2 Faridabad – Badarpur
    (28.5405, 77.1220),  # NH-8 Mahipalpur – Gurugram Toll
    (28.6733, 77.2286),  # NH-44 Kashmere Gate – Karnal Rd
    (28.6038, 77.2867),  # Noida Link Rd Akshardham – Mayur Vihar
    (28.7162, 77.2584),  # Wazirabad Rd – Signature Bridge
    (28.556, 77.100),  # NH8
    (28.650, 77.250),  # NH24
    (28.600, 77.200),  # Ring Road
    (28.632, 77.219),  # Connaught Place
    (28.580, 77.050),  # Dwarka
    (28.650, 77.315),  # Anand Vihar
    (28.650, 77.200),  # Karol Bagh
    (28.580, 77.250),  # Lajpat Nagar
    (28.720, 77.110),  # Rohini
    (28.595, 77.300),  # Mayur Vihar
    (28.530, 77.200),  # Vasant Kunj
    (28.580, 77.250),  # Sarai Kale Khan
    (28.660, 77.150),  # Punjabi Bagh
    (28.556, 77.100),  # Indira Gandhi Airport
    (28.650, 77.290),  # East Delhi
    (28.580, 77.210),  # South Extension
    (28.520, 77.210),  # Saket
    (28.650, 77.260),  # Laxmi Nagar
    (28.690, 77.200),  # Mukherjee Nagar
    (28.690, 77.150),  # Shalimar Bagh
]

LAT_MIN, LAT_MAX = 28.40, 28.88
LON_MIN, LON_MAX = 76.84, 77.33
GRID_SIZE = 20

TIER2_POINTS=[]
lat_stp=(LAT_MAX-LAT_MIN)/(GRID_SIZE-1)
lon_stp=(LON_MAX-LON_MIN)/(GRID_SIZE-1)

for i in range(GRID_SIZE):
    for j in range(GRID_SIZE):
        lat=LAT_MIN+i*lat_stp
        lon=LON_MIN+j*lon_stp
        TIER2_POINTS.append((lat,lon))

BATCH_SIZE = 20
TIER2_BATCHES = [TIER2_POINTS[i:i+BATCH_SIZE] for i in range(0, len(TIER2_POINTS), BATCH_SIZE)]
NUM_BATCHES = len(TIER2_BATCHES)

hdfs_client=InsecureClient("http://localhost:9870",user="hdoop")
producer=KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

running = True
batch_counter = 0

def handle_exit(signum, frame):
    global running
    print("\n Shutting down gracefully...")
    running = False
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


api_index=0
def get_api_key():
    global api_index
    key=API_KEYS[api_index]
    api_index=(api_index+1)%len(API_KEYS)
    return key

def fetch_data(lat,lon):
        url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        params={"point":f"{lat},{lon}" ,"key":get_api_key()}
        try:
             r=requests.get(url,params=params,timeout=10)
             if r.status_code==200:
                  data=r.json()
                  data["timestamp"]=datetime.utcnow().isoformat()
                  data["location"] = {"lat": lat, "lon": lon}
                  return data         
             else:
                  return{"error":r.status_code,"point":(lat,lon)}
        except Exception as e:
             return {"error":str(e),"point":(lat,lon)}

def save_hdfs(path_prefix, records):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"{path_prefix}/traffic_{ts}.json"
    try:
        with hdfs_client.write(path, encoding="utf-8") as writer:
            writer.write(json.dumps(records))
        print(f" Saved {len(records)} → {path}")
    except Exception as e:
        print(f" HDFS write error: {e}")

def push_kafka(topic, records):
    try:
        for rec in records:
            producer.send(topic, rec)
        producer.flush()
        print(f" Pushed {len(records)} → {topic}")
    except Exception as e:
        print(f" Kafka error: {e}")

#sheduleing 
def fetch_tier1():
    print(f"\n[Tier1] Fetching {len(TIER1_POINTS)} busiest points...")
    records = [r for lat, lon in TIER1_POINTS if (r := fetch_data(lat, lon))]
    if records:
        save_hdfs("/traffic/dashboard/tier1", records)
        push_kafka(TOPIC_TIER1, records)

def fetch_tier2_batch():
    global batch_counter
    batch = TIER2_BATCHES[batch_counter]
    print(f"\n[Tier2-Batch] Fetching batch {batch_counter+1}/{NUM_BATCHES}...")
    records = [r for lat, lon in batch if (r := fetch_data(lat, lon))]
    if records:
        save_hdfs("/traffic/dashboard/tier2_batch", records)
        push_kafka(TOPIC_TIER2_BATCH, records)
    batch_counter = (batch_counter + 1) % NUM_BATCHES

def fetch_tier2_snapshot():
    print(f"\n[Tier2-Snapshot] Fetching ALL {len(TIER2_POINTS)} points for ML...")
    records = [r for lat, lon in TIER2_POINTS if (r := fetch_data(lat, lon))]
    if records:
        save_hdfs("/traffic/ml/tier2_snapshot", records)
        push_kafka(TOPIC_TIER2_SNAPSHOT, records)

#threading
def run_in_thread(job_func):
    t = threading.Thread(target=job_func)
    t.daemon = True
    t.start()

def start_scheduler():
    # Every 5 min → Tier1 + Tier2 batch
    schedule.every(5).minutes.do(run_in_thread, job_func=fetch_tier1)
    schedule.every(5).minutes.do(run_in_thread, job_func=fetch_tier2_batch)
    # Every 75 min → Snapshot
    schedule.every(75).minutes.do(run_in_thread, job_func=fetch_tier2_snapshot)

    print(f"Streaming started | Tier1: {len(TIER1_POINTS)} | Tier2: {len(TIER2_POINTS)} points | {NUM_BATCHES} batches")
    print(" Tier1 & Tier2 batches every 5 min | Snapshots every 75 min\n")
    while running:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    # Run immediately once at startup
    run_in_thread(fetch_tier1)
    run_in_thread(fetch_tier2_batch)
    run_in_thread(fetch_tier2_snapshot)
    # Then keep running with schedule
    start_scheduler()

                    
        
            



