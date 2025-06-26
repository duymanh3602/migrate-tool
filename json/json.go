package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Configuration struct
type MigrationConfig struct {
	ConnectionURI  string
	DatabaseName   string
	CollectionName string
	FieldName      string
	NewFieldName   string // Optional: if you want to create a new field instead of updating existing
	BatchSize      int64
	DryRun         bool
}

// Document represents a generic MongoDB document
type Document map[string]interface{}

// func main() {
// 	// Configuration - modify these values according to your setup
// 	config := MigrationConfig{
// 		ConnectionURI:  "mongodb://localhost:27017", // Change to your MongoDB URI
// 		DatabaseName:   "lms_dev",                   // Change to your database name
// 		CollectionName: "NewCourseLessonItem",       // Change to your collection name
// 		FieldName:      "Content",                   // Field containing JSON string
// 		NewFieldName:   "",                          // Leave empty to update same field, or specify new field name
// 		BatchSize:      100,                         // Process documents in batches
// 		DryRun:         true,                        // Set to false to actually perform migration
// 	}

// 	if err := runMigration(config); err != nil {
// 		log.Fatal("Migration failed:", err)
// 	}
// }

func runMigration(config MigrationConfig) error {
	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.ConnectionURI))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	// Test connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	collection := client.Database(config.DatabaseName).Collection(config.CollectionName)

	// Count total documents that need migration
	filter := bson.M{
		config.FieldName: bson.M{"$type": "string", "$ne": ""},
	}

	totalCount, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to count documents: %w", err)
	}

	fmt.Printf("Found %d documents to migrate\n", totalCount)
	if totalCount == 0 {
		fmt.Println("No documents to migrate")
		return nil
	}

	if config.DryRun {
		fmt.Println("DRY RUN MODE - No actual changes will be made")
		return previewMigration(ctx, collection, config, filter)
	}

	// Perform actual migration
	return performMigration(ctx, collection, config, filter)
}

func previewMigration(ctx context.Context, collection *mongo.Collection, config MigrationConfig, filter bson.M) error {
	// Show sample of documents that will be migrated
	cursor, err := collection.Find(ctx, filter, options.Find().SetLimit(5))
	if err != nil {
		return fmt.Errorf("failed to find documents: %w", err)
	}
	defer cursor.Close(ctx)

	fmt.Println("\nSample documents that will be migrated:")
	for cursor.Next(ctx) {
		var doc Document
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("Failed to decode document: %v", err)
			continue
		}

		fmt.Printf("Document ID: %v\n", doc["_id"])
		fmt.Printf("Current %s: %v\n", config.FieldName, doc[config.FieldName])

		// Try to parse the JSON string
		if jsonStr, ok := doc[config.FieldName].(string); ok {
			var jsonObj interface{}
			if err := json.Unmarshal([]byte(jsonStr), &jsonObj); err != nil {
				fmt.Printf("⚠️  Invalid JSON in document %v: %v\n", doc["_id"], err)
			} else {
				fmt.Printf("✅ Valid JSON - will be converted to: %v\n", jsonObj)
			}
		}
		fmt.Println("---")
	}

	return nil
}

func performMigration(ctx context.Context, collection *mongo.Collection, config MigrationConfig, filter bson.M) error {
	fmt.Println("Starting migration...")

	var processed int64
	var successful int64
	var failed int64

	// Use aggregation with batch processing
	pipeline := []bson.M{
		{"$match": filter},
		{"$limit": config.BatchSize},
	}

	for {
		cursor, err := collection.Aggregate(ctx, pipeline)
		if err != nil {
			return fmt.Errorf("failed to create aggregation cursor: %w", err)
		}

		var batchProcessed int64
		var bulkOps []mongo.WriteModel

		for cursor.Next(ctx) {
			var doc Document
			if err := cursor.Decode(&doc); err != nil {
				log.Printf("Failed to decode document: %v", err)
				failed++
				continue
			}

			// Process the document
			updateDoc, err := processDocument(doc, config)
			if err != nil {
				log.Printf("Failed to process document %v: %v", doc["_id"], err)
				failed++
				continue
			}

			if updateDoc != nil {
				// Create update operation
				updateOp := mongo.NewUpdateOneModel().
					SetFilter(bson.M{"_id": doc["_id"]}).
					SetUpdate(bson.M{"$set": updateDoc})
				bulkOps = append(bulkOps, updateOp)
				batchProcessed++
			}
		}
		cursor.Close(ctx)

		// Execute bulk operations
		if len(bulkOps) > 0 {
			result, err := collection.BulkWrite(ctx, bulkOps)
			if err != nil {
				log.Printf("Bulk write failed: %v", err)
				failed += batchProcessed
			} else {
				successful += result.ModifiedCount
				fmt.Printf("Processed batch: %d successful updates\n", result.ModifiedCount)
			}
		}

		processed += batchProcessed

		// Check if we've processed all documents or if batch was smaller than expected
		if batchProcessed < config.BatchSize {
			break
		}

		// Update skip for next batch
		pipeline[1] = bson.M{"$skip": processed}
	}

	fmt.Printf("\nMigration completed!\n")
	fmt.Printf("Total processed: %d\n", processed)
	fmt.Printf("Successful: %d\n", successful)
	fmt.Printf("Failed: %d\n", failed)

	return nil
}

func processDocument(doc Document, config MigrationConfig) (Document, error) {
	jsonStr, ok := doc[config.FieldName].(string)
	if !ok {
		return nil, fmt.Errorf("field %s is not a string", config.FieldName)
	}

	// Parse JSON string
	var jsonObj interface{}
	if err := json.Unmarshal([]byte(jsonStr), &jsonObj); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	// Determine target field name
	targetField := config.FieldName
	if config.NewFieldName != "" {
		targetField = config.NewFieldName
	}

	// Create update document
	updateDoc := Document{
		targetField: jsonObj,
	}

	// If creating a new field, also remove the old field
	if config.NewFieldName != "" && config.NewFieldName != config.FieldName {
		updateDoc[config.FieldName] = nil // This will be handled with $unset in a separate operation
	}

	return updateDoc, nil
}

// Helper function to create a backup collection (optional)
func createBackup(ctx context.Context, client *mongo.Client, config MigrationConfig) error {
	sourceCollection := client.Database(config.DatabaseName).Collection(config.CollectionName)
	backupCollectionName := fmt.Sprintf("%s_backup_%d", config.CollectionName, time.Now().Unix())

	fmt.Printf("Creating backup collection: %s\n", backupCollectionName)

	// This is a simple approach - for large collections, consider using MongoDB's built-in backup tools
	cursor, err := sourceCollection.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to read source collection: %w", err)
	}
	defer cursor.Close(ctx)

	backupCollection := client.Database(config.DatabaseName).Collection(backupCollectionName)
	var docs []interface{}

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		docs = append(docs, doc)

		// Insert in batches to avoid memory issues
		if len(docs) >= 1000 {
			if _, err := backupCollection.InsertMany(ctx, docs); err != nil {
				return fmt.Errorf("failed to insert backup documents: %w", err)
			}
			docs = docs[:0] // Clear slice
		}
	}

	// Insert remaining documents
	if len(docs) > 0 {
		if _, err := backupCollection.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("failed to insert remaining backup documents: %w", err)
		}
	}

	fmt.Printf("Backup created successfully: %s\n", backupCollectionName)
	return nil
}
