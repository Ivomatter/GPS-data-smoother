import csv
import json
import random
import threading
import time
from kafka import KafkaProducer
from collections import defaultdict
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

CSV_FILE = '../data/raw_gps.csv'
TOPIC = 'raw_gps_data'

partition_vehicle_count = defaultdict(int)
vehicle_to_partition = {}

def load_data():
    data_by_vehicle = defaultdict(list)
    with open(CSV_FILE, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            veh_id = row['veh_id']
            data_by_vehicle[veh_id].append(row)
    return data_by_vehicle

def preprocess_record(record):
    default_values = {
        'int': -1,          
        'float': -1.0,       
        'string': "",       
        'bool': False        
    }
    
    field_types = {
        'alt': 'float',
        'lat': 'float',
        'lon': 'float',
        'sats': 'int',
        'signal': 'int',
        'speed': 'float',
        'ts': 'int',
        'veh_id': 'string',
        'dev_id': 'string',
        'date': 'string',
        'task_id': 'int',
        'next_task_id': 'int',
        'line_id': 'string',
        'route_id': 'string',
        'driver_id': 'string',
        'car_no': 'int',
        'course_no': 'int',
        'pt': 'string',
        'primary_veh_id': 'string',
        'prev_stop_id': 'string',
        'prev_stop_dev': 'float',
        'cur_stop_id': 'string',
        'cur_stop_dev': 'float',
        'seg_fraction': 'float',
        'status': 'string',
        'in_depot': 'bool'
    }

    for field, field_type in field_types.items():

        if field not in record:
            record[field] = default_values[field_type]
            continue
            
       
        if record[field] == "" or record[field] is None:
            record[field] = default_values[field_type]
            continue
        
        try:
            if field_type == 'int':
                record[field] = int(float(record[field])) 
            elif field_type == 'float':
                record[field] = float(record[field])
            elif field_type == 'bool':
                record[field] = record[field].lower() == 'true'
        except (ValueError, TypeError):
            record[field] = default_values[field_type]
    
    return record

def stream_vehicle_data(veh_id, records, partition_count=3):

    partition = get_least_loaded_partition(veh_id, partition_count)
    
    print(f"Vehicle {veh_id} assigned to partition {partition}")
    
    for record in records:
        processed_record = preprocess_record(record)
        producer.send(
            TOPIC, 
            key=veh_id.encode('utf-8'),
            value=processed_record,
            partition=partition  
        )
        # print(f"Sent for vehicle {veh_id} to partition {partition}: {processed_record['lat']}, {processed_record['lon']}")
        time.sleep(random.uniform(0, 2))  # Sleep 0–2s


def get_least_loaded_partition(veh_id, partition_count=3):
    if veh_id in vehicle_to_partition:
        return vehicle_to_partition[veh_id]
    
    min_partition = min(range(partition_count), key=lambda p: partition_vehicle_count[p])
    vehicle_to_partition[veh_id] = min_partition
    partition_vehicle_count[min_partition] += 1
    return min_partition

def create_topic(topic_name, num_partitions=3):
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='topic_creator')
    
    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=1,
        topic_configs={
            "cleanup.policy": "compact",
            "delete.retention.ms": "1000"
        }
    )
    
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    finally:
        admin_client.close()

def main():
    create_topic(TOPIC, num_partitions=3)
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