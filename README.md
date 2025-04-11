Create the raw_gps_data topic for producing raw data

docker exec -it kafka-setup-kafka-1 bash
kafka-topics --create --topic raw_gps_data --bootstrap-server localhost:9092 --partitions 3 --config "cleanup.policy=compact" --config "delete.retention.ms=1000"

1. Begin producing raw csv data by running /producer/producer.py
python3 producer.py

2. Begin consuming from raw_gps_data using Go client /OSRM/client.go
go run client.go

3. Once streaming, fire up /consumer/output.py which reads from processed_gps_data (creted from client.go) and serializes it to csv and json
python3 output.py

4. Run the visualiser to create two maps - one routing the raw data, and one routing the processed data.
python3 visualiser.py

5. Open raw_gps_data_map.html and processed_gps_data_map.html


kafka-topics --delete --bootstrap-server localhost:9092 --topic raw_gps_data