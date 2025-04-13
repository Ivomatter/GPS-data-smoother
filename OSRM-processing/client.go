package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// GPSRecord represents a single GPS data point
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

const MISSING_INT = -1

// BatchProcessor handles processing of GPS data in batches
type BatchProcessor struct {
	records        []*GPSRecord
	vehicleContext map[string]*VehicleContext
	routeMapping   map[string]string // line_id -> route_id mapping
	mutex          sync.Mutex
	batchSize      int
	producer       *kafka.Producer
	outputTopic    string
}

// VehicleContext maintains state for a specific vehicle
type VehicleContext struct {
	LastKnownLineID     string
	LastKnownRouteID    string
	LastKnownTaskID     int
	LastKnownNextTaskID int
	LastKnownPrevStopID string
	LastKnownCurStopID  string
	LastSegFraction     float64
	LastLat             float64
	LastLon             float64
	LastTimestamp       int64
	RoutePoints         [][]float64       // For calculating seg_fraction
	TaskHistory         map[int]int       // task_id -> next_task_id mapping
	LineRouteMapping    map[string]string // line_id -> route_id mapping
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(batchSize int, producer *kafka.Producer, outputTopic string) *BatchProcessor {
	return &BatchProcessor{
		records:        make([]*GPSRecord, 0, batchSize),
		vehicleContext: make(map[string]*VehicleContext),
		routeMapping:   make(map[string]string),
		batchSize:      batchSize,
		producer:       producer,
		outputTopic:    outputTopic,
	}
}

// AddRecord adds a record to the batch, processes if batch size is reached
func (bp *BatchProcessor) AddRecord(record *GPSRecord) error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bp.records = append(bp.records, record)

	// Ensure we have a context for this vehicle
	if _, exists := bp.vehicleContext[record.VehicleID]; !exists {
		bp.vehicleContext[record.VehicleID] = &VehicleContext{
			TaskHistory:      make(map[int]int),
			LineRouteMapping: make(map[string]string),
		}
	}

	// Update context with this record's data
	bp.updateContext(record)

	// If we've reached batch size, process the batch
	if len(bp.records) >= bp.batchSize {
		return bp.processBatch()
	}

	return nil
}

// updateContext updates vehicle context with current record data
func (bp *BatchProcessor) updateContext(record *GPSRecord) {
	ctx := bp.vehicleContext[record.VehicleID]

	// Update line_id -> route_id mapping if we have both
	if record.LineID != "" && record.RouteID != "" {
		ctx.LineRouteMapping[record.LineID] = record.RouteID
		bp.routeMapping[record.LineID] = record.RouteID
	}

	// Update task_id -> next_task_id mapping if we have both
	if record.TaskID != MISSING_INT && record.NextTaskID != MISSING_INT {
		ctx.TaskHistory[record.TaskID] = record.NextTaskID
	}

	// Update last known values for this vehicle
	if record.LineID != "" {
		ctx.LastKnownLineID = record.LineID
	}
	if record.RouteID != "" {
		ctx.LastKnownRouteID = record.RouteID
	}
	if record.TaskID != MISSING_INT {
		ctx.LastKnownTaskID = record.TaskID
	}
	if record.NextTaskID != MISSING_INT {
		ctx.LastKnownNextTaskID = record.NextTaskID
	}
	if record.PrevStopID != "" {
		ctx.LastKnownPrevStopID = record.PrevStopID
	}
	if record.CurStopID != "" {
		ctx.LastKnownCurStopID = record.CurStopID
	}

	ctx.LastLat = record.Lat
	ctx.LastLon = record.Lon
	ctx.LastTimestamp = record.Timestamp
}

// imputeMissingData fills in missing fields based on context
func (bp *BatchProcessor) imputeMissingData() {
	for _, record := range bp.records {
		ctx := bp.vehicleContext[record.VehicleID]

		// Impute route_id from line_id if missing
		if record.RouteID == "" && record.LineID != "" {
			if routeID, exists := bp.routeMapping[record.LineID]; exists {
				record.RouteID = routeID
			} else if routeID, exists := ctx.LineRouteMapping[record.LineID]; exists {
				record.RouteID = routeID
			}
		}

		if record.PrevStopID == "" && ctx.LastKnownCurStopID != "" {
			if record.CurStopID == ctx.LastKnownCurStopID {
				record.PrevStopID = ctx.LastKnownPrevStopID
			} else {
				record.PrevStopID = ctx.LastKnownCurStopID
			}
		}

		// Impute task_id if we know the previous task's next_task_id
		if record.TaskID == MISSING_INT && ctx.LastKnownNextTaskID != MISSING_INT {
			if record.NextTaskID == ctx.LastKnownNextTaskID {
				record.TaskID = ctx.LastKnownTaskID
			} else {
				record.TaskID = ctx.LastKnownNextTaskID
			}
		}

		// Impute next_task_id if we know this task's next task from history
		if record.NextTaskID == MISSING_INT && record.TaskID != MISSING_INT {
			if nextTask, exists := ctx.TaskHistory[record.TaskID]; exists {
				record.NextTaskID = nextTask
			}
		}

		if record.Status == "on break" {
			record.SegFraction = 1.0
		}

		// Update last seg_fraction if we have a valid one
		if record.SegFraction > 0 {
			ctx.LastSegFraction = record.SegFraction
		}
	}
}

// processBatch processes the current batch through OSRM and publishes to Kafka
func (bp *BatchProcessor) processBatch() error {
	// First impute missing data based on context
	bp.imputeMissingData()

	// Group records by vehicle for OSRM batching
	vehicleGroups := make(map[string][]*GPSRecord)
	for _, record := range bp.records {
		vehicleGroups[record.VehicleID] = append(vehicleGroups[record.VehicleID], record)
	}

	// Process each vehicle's points with OSRM
	for vehID, records := range vehicleGroups {
		err := bp.processWithOSRM(vehID, records)
		if err != nil {
			log.Printf("Error processing with OSRM: %v", err)
		}
	}

	// After processing, publish all records to output Kafka topic
	for _, record := range bp.records {
		err := bp.publishToKafka(record)
		if err != nil {
			log.Printf("Error publishing to Kafka: %v", err)
		}
	}

	// Clear the batch
	bp.records = make([]*GPSRecord, 0, bp.batchSize)

	return nil
}

// processWithOSRM sends coordinates to OSRM for matching and smoothing
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

		// Dynamically adjust radius based on GPS quality (optional)
		radius := 50 // Default radius in meters
		if record.Signal > 4 {
			radius = 20
		} else if record.Signal < 2 {
			radius = 100
		}
		radiusStr.WriteString(fmt.Sprintf("%d", radius))
	}

	// Build the full OSRM match URL
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

		if i == 0 {
			record.SegFraction = 0.0
		} else if i == len(records)-1 {
			record.SegFraction = 1.0
		} else if len(osrmResponse.Matchings) > 0 {
			matchingIdx := osrmResponse.Tracepoints[i].MatchingsIndex
			if matchingIdx >= 0 && matchingIdx < len(osrmResponse.Matchings) {
				waypointIndex := -1
				for j, wp := range osrmResponse.Matchings[matchingIdx].Waypoints {
					if wp.WaypointIndex == i {
						waypointIndex = j
						break
					}
				}
				if waypointIndex >= 0 {
					totalWaypoints := len(osrmResponse.Matchings[matchingIdx].Waypoints)
					if totalWaypoints > 1 {
						record.SegFraction = roundToThirdDecimal(float64(waypointIndex) / float64(totalWaypoints-1))
					}
				}
			}
		}

		if record.Status == "on break" {
			record.SegFraction = 1.0
		}
	}

	return nil
}

// publishToKafka sends a processed record to the output Kafka topic
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
	// Configuration
	inputTopic := "raw_gps_data"
	outputTopic := "processed_gps_data"
	batchSize := 15
	consumerCount := 3

	// Create Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel for OS signals
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Start consumers
	var wg sync.WaitGroup
	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		processor := NewBatchProcessor(batchSize, producer, outputTopic)
		go startConsumer(ctx, i, inputTopic, processor, &wg)
	}

	// Wait for interrupt signal
	<-sigchan
	log.Println("Received termination signal, shutting down...")

	// Cancel context to signal consumers to stop
	cancel()

	// Wait for consumers to finish
	wg.Wait()
	log.Println("All consumers stopped, exiting")
}

// startConsumer initializes a Kafka consumer and processes incoming messages
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

	// Track vehicle IDs this consumer is handling
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

			// Extract vehicle ID from message key
			vehicleID := string(msg.Key)
			if vehicleID == "" {
				log.Printf("Consumer %d: Received message without vehicle ID", id)
				continue
			}

			// Track this vehicle if new
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

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func roundToThirdDecimal(val float64) float64 {
	return math.Round(val*1000) / 1000
}
