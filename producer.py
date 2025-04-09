import csv
import json
import random
import threading
import time
from kafka import KafkaProducer
from collections import defaultdict

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

CSV_FILE = 'raw_gps.csv'
TOPIC = 'raw_gps_data'

# Read and group rows by veh_id
def load_data():
    data_by_vehicle = defaultdict(list)
    with open(CSV_FILE, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            veh_id = row['veh_id']
            data_by_vehicle[veh_id].append(row)
    return data_by_vehicle

# Function to send GPS data for a single vehicle
def stream_vehicle_data(veh_id, records):
    for record in records:
        producer.send(TOPIC, record)
        print(f"Sent for vehicle {veh_id}: {record['lat']}, {record['lon']}")
        time.sleep(random.uniform(0, 2))  # Sleep 0–2s

def main():
    data_by_vehicle = load_data()
    threads = []
    for veh_id, records in data_by_vehicle.items():
        t = threading.Thread(target=stream_vehicle_data, args=(veh_id, records))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("All vehicles done.")

if __name__ == '__main__':
    main()