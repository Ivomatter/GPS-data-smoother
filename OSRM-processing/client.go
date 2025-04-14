package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type GPSRecord struct {
	Alt          float64 `json:"alt"`
	Lat          float64 `json:"lat"`
	Lon          float64 `json:"lon"`
	Sats         int     `json:"sats"`
	Signal       int     `json:"signal"`
	Speed        float64 `json:"speed"`
	Timestamp    int64   `json:"ts"`
	VehicleID    string  `json:"veh_id"`
	DeviceID     string  `json:"dev_id"`
	Date         string  `json:"date"`
	TaskID       int     `json:"task_id"`
	NextTaskID   int     `json:"next_task_id"`
	LineID       string  `json:"line_id"`
	RouteID      string  `json:"route_id"`
	DriverID     string  `json:"driver_id"`
	CarNo        int     `json:"car_no"`
	CourseNo     float64 `json:"course_no"`
	Pt           string  `json:"pt"`
	PrimaryVehID string  `json:"primary_veh_id"`
	PrevStopID   string  `json:"prev_stop_id"`
	PrevStopDev  float64 `json:"prev_stop_dev"`
	CurStopID    string  `json:"cur_stop_id"`
	CurStopDev   float64 `json:"cur_stop_dev"`
	SegFraction  float64 `json:"seg_fraction"`
	Status       string  `json:"status"`
	InDepot      bool    `json:"in_depot"`
}

type BatchProcessor struct {
	records     []*GPSRecord
	mutex       sync.Mutex
	batchSize   int
	producer    *kafka.Producer
	outputTopic string
}

func NewBatchProcessor(batchSize int, producer *kafka.Producer, outputTopic string) *BatchProcessor {
	return &BatchProcessor{
		records:     make([]*GPSRecord, 0, batchSize),
		batchSize:   batchSize,
		producer:    producer,
		outputTopic: outputTopic,
	}
}

func (bp *BatchProcessor) AddRecord(record *GPSRecord) error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bp.records = append(bp.records, record)

	if len(bp.records) >= bp.batchSize {
		return bp.processBatch()
	}

	return nil
}

func (bp *BatchProcessor) processBatch() error {
	vehicleGroups := make(map[string][]*GPSRecord)
	filteredRecords := make([]*GPSRecord, 0, len(bp.records))

	for _, record := range bp.records {
		if record.Speed >= 1.0 && record.Signal >= 27 && record.Sats > 7 {
			vehicleGroups[record.VehicleID] = append(vehicleGroups[record.VehicleID], record)
			filteredRecords = append(filteredRecords, record)
		} else if !(record.Speed == 0 && record.SegFraction < 0.01) {
			vehicleGroups[record.VehicleID] = append(vehicleGroups[record.VehicleID], record)
			filteredRecords = append(filteredRecords, record)
		}
	}

	for vehID, records := range vehicleGroups {
		err := bp.processWithOSRM(vehID, records)
		if err != nil {
			log.Printf("Error processing with OSRM: %v", err)
		}
	}

	for _, record := range filteredRecords {
		err := bp.publishToKafka(record)
		if err != nil {
			log.Printf("Error publishing to Kafka: %v", err)
		}
	}

	bp.records = make([]*GPSRecord, 0, bp.batchSize)

	return nil
}

func (bp *BatchProcessor) processWithOSRM(vehicleID string, records []*GPSRecord) error {

	if len(records) <= 1 {
		return nil // Need at least 2 points for matching
	}

	coordinates := make([][]float64, len(records))
	timestamps := make([]int64, len(records))

	var coordStr, tsStr, radiusStr strings.Builder

	for i, record := range records {
		coordinates[i] = []float64{record.Lon, record.Lat}
		timestamps[i] = record.Timestamp / 1000 // OSRM expects seconds

		if i > 0 {
			coordStr.WriteString(";")
			tsStr.WriteString(";")
			radiusStr.WriteString(";")
		}

		coordStr.WriteString(fmt.Sprintf("%.6f,%.6f", record.Lon, record.Lat))
		tsStr.WriteString(fmt.Sprintf("%d", timestamps[i]))

		radius := 30
		if record.Signal >= 30 && record.Sats > 9 {
			radius = 15
		} else if record.Signal < 20 || record.Sats <= 5 {
			radius = 100
		}
		radiusStr.WriteString(fmt.Sprintf("%d", radius))
	}

	osrmURL := fmt.Sprintf("http://localhost:5000/match/v1/driving/%s?timestamps=%s&radiuses=%s&geometries=geojson&overview=full&gaps=split&tidy=true",
		coordStr.String(), tsStr.String(), radiusStr.String())

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(osrmURL)
	if err != nil {
		return fmt.Errorf("OSRM request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OSRM request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var osrmResponse struct {
		Code      string `json:"code"`
		Matchings []struct {
			Confidence float64 `json:"confidence"`
			Geometry   struct {
				Coordinates [][]float64 `json:"coordinates"`
			} `json:"geometry"`
			Legs []struct {
				Steps []struct {
					Intersections []struct {
						Location []float64 `json:"location"`
					} `json:"intersections"`
				} `json:"steps"`
			} `json:"legs"`
			Distance  float64 `json:"distance"`
			Duration  float64 `json:"duration"`
			Waypoints []struct {
				WaypointIndex int       `json:"waypoint_index"`
				Location      []float64 `json:"location"`
				Name          string    `json:"name"`
			} `json:"waypoints"`
		} `json:"matchings"`
		Tracepoints []struct {
			AlternativesCount int       `json:"alternatives_count"`
			WaypointIndex     int       `json:"waypoint_index"`
			MatchingsIndex    int       `json:"matchings_index"`
			Location          []float64 `json:"location"`
			Name              string    `json:"name"`
		} `json:"tracepoints"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&osrmResponse); err != nil {
		return fmt.Errorf("failed to parse OSRM response: %v", err)
	}

	if osrmResponse.Code != "Ok" || len(osrmResponse.Matchings) == 0 {
		return fmt.Errorf("OSRM returned invalid response: %s", osrmResponse.Code)
	}

	for i, record := range records {
		if i >= len(osrmResponse.Tracepoints) || osrmResponse.Tracepoints[i].Location == nil {
			continue
		}

		matchedLon := osrmResponse.Tracepoints[i].Location[0]
		matchedLat := osrmResponse.Tracepoints[i].Location[1]

		record.Lon = matchedLon
		record.Lat = matchedLat

	}

	return nil
}

func (bp *BatchProcessor) publishToKafka(record *GPSRecord) error {
	jsonData, err := json.Marshal(record)
	if err != nil {
		return err
	}

	return bp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &bp.outputTopic,
			Partition: kafka.PartitionAny,
		},
		Value: jsonData,
		Key:   []byte(record.VehicleID),
	}, nil)
}

func main() {
	inputTopic := "raw_gps_data"
	outputTopic := "processed_gps_data"
	batchSize := 15
	consumerCount := 3

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		processor := NewBatchProcessor(batchSize, producer, outputTopic)
		go startConsumer(ctx, i, inputTopic, processor, &wg)
	}

	<-sigchan
	log.Println("Received termination signal, shutting down...")

	cancel()

	wg.Wait()
	log.Println("All consumers stopped, exiting")
}

func startConsumer(ctx context.Context, id int, topic string, processor *BatchProcessor, wg *sync.WaitGroup) {
	defer wg.Done()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":             "localhost:9092",
		"group.id":                      "gps-processor-group",
		"auto.offset.reset":             "latest",
		"enable.auto.commit":            false,
		"partition.assignment.strategy": "cooperative-sticky",
	})
	if err != nil {
		log.Fatalf("Consumer %d: Failed to create consumer: %v", id, err)
	}
	defer consumer.Close()

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Consumer %d: Failed to subscribe: %v", id, err)
	}

	handledVehicles := make(map[string]bool)

	log.Printf("Consumer %d started and waiting for messages", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Consumer %d shutting down (handled vehicles: %v)", id, handledVehicles)
			return

		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer %d error: %v", id, err)
				continue
			}

			vehicleID := string(msg.Key)
			if vehicleID == "" {
				log.Printf("Consumer %d: Received message without vehicle ID", id)
				continue
			}

			if !handledVehicles[vehicleID] {
				handledVehicles[vehicleID] = true
				log.Printf("Consumer %d now handling vehicle %s", id, vehicleID)
			}

			var record GPSRecord
			if err := json.Unmarshal(msg.Value, &record); err != nil {
				log.Printf("Consumer %d: Error parsing message: %v", id, err)
				continue
			}

			if err := processor.AddRecord(&record); err != nil {
				log.Printf("Consumer %d: Error processing record: %v", id, err)
				continue
			}

			if _, err := consumer.CommitMessage(msg); err != nil {
				log.Printf("Consumer %d: Failed to commit offset: %v", id, err)
			}
		}
	}
}
