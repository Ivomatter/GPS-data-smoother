import json
from kafka import KafkaConsumer
import pandas as pd
import time

consumer = KafkaConsumer(
    'processed_gps_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,             
    group_id='exporter-consumer-group',   
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000
)

def export_kafka_to_file():
    print("Starting to consume processed GPS data...")
    records = []
    
    try:
        for message in consumer:
            record = message.value
            records.append(record)
            print(f"Received record for vehicle {record['veh_id']}")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()
    
    if records:
        # Export to both CSV and JSON
        records.sort(key=lambda r: r['veh_id'])
        df = pd.DataFrame(records)
        df.to_csv('../data/processed_gps_data.csv', index=False)
        
        with open('../data/processed_gps_data.json', 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"Exported {len(records)} records to CSV and JSON files")
        return df
    else:
        print("No records found to export")
        return None

if __name__ == '__main__':
    export_kafka_to_file()