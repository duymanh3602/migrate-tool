package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Lấy toàn bộ user ID từ MySQL và lưu vào map
func getAllUserIDsFromMySQL(mysqlDB *sql.DB) (map[string]struct{}, error) {
	userMap := make(map[string]struct{})

	rows, err := mysqlDB.Query("SELECT Id FROM AspNetUsers")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}
		userMap[id] = struct{}{}
	}

	return userMap, nil
}

func cleanInvalidUserAssignments(mongoURI, dbName, mysqlDSN string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// MongoDB
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return err
	}
	defer mongoClient.Disconnect(ctx)

	db := mongoClient.Database(dbName)
	itemAssignmentCol := db.Collection("ItemAssignmentData")

	// MySQL
	mysqlDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return err
	}
	defer mysqlDB.Close()

	// Lấy danh sách hợp lệ từ MySQL
	validUserIDs, err := getAllUserIDsFromMySQL(mysqlDB)
	if err != nil {
		return err
	}
	log.Printf("Loaded %d valid user IDs from MySQL", len(validUserIDs))

	// Duyệt tất cả document trong ItemAssignmentData
	cursor, err := itemAssignmentCol.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("Decode error: %v", err)
			continue
		}

		userId, ok := doc["UserId"].(string)
		if !ok {
			log.Printf("UserId không hợp lệ: %v", doc["UserId"])
			continue
		}

		// Nếu không có trong danh sách hợp lệ → xóa
		if _, exists := validUserIDs[userId]; !exists {
			id, ok := doc["_id"].(primitive.ObjectID)
			if !ok {
				log.Printf("Document _id không hợp lệ: %v", doc["_id"])
				continue
			}

			_, err := itemAssignmentCol.DeleteOne(ctx, bson.M{"_id": id})
			if err != nil {
				log.Printf("Xoá thất bại _id=%v: %v", id.Hex(), err)
			} else {
				log.Printf("❌ Đã xoá document với UserId không hợp lệ: %s", userId)
			}
		}
	}

	return nil
}

// func main() {
// 	if err := godotenv.Load(); err != nil {
// 		log.Fatal("Lỗi khi đọc .env file")
// 	}

// 	mongoURI := os.Getenv("MONGO_URI")
// 	dbName := os.Getenv("DB_NAME")
// 	mysqlDSN := os.Getenv("MYSQL_DSN")

// 	if mongoURI == "" || dbName == "" || mysqlDSN == "" {
// 		log.Fatal("Thiếu biến môi trường: MONGO_URI, DB_NAME, MYSQL_DSN")
// 	}

// 	if err := cleanInvalidUserAssignments(mongoURI, dbName, mysqlDSN); err != nil {
// 		log.Fatalf("Lỗi thực thi: %v", err)
// 	}
// }


func moveInvalidUserAssignments(mongoURI, dbName, mysqlDSN string) error {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()

    // MongoDB setup
    mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
    if err != nil {
        return err
    }
    defer mongoClient.Disconnect(ctx)

    db := mongoClient.Database(dbName)
    itemAssignmentCol := db.Collection("ItemAssignmentData")
    invalidAssignmentCol := db.Collection("ValidItemAssignmentData")

    // MySQL setup
    mysqlDB, err := sql.Open("mysql", mysqlDSN)
    if err != nil {
        return err
    }
    defer mysqlDB.Close()

    // Load valid user IDs from MySQL
    validUserIDs, err := getAllUserIDsFromMySQL(mysqlDB)
    if err != nil {
        return err
    }
    log.Printf("Loaded %d valid user IDs from MySQL", len(validUserIDs))

    cursor, err := itemAssignmentCol.Find(ctx, bson.M{})
    if err != nil {
        return err
    }
    defer cursor.Close(ctx)

    for cursor.Next(ctx) {
        var doc bson.M
        if err := cursor.Decode(&doc); err != nil {
            log.Printf("Decode error: %v", err)
            continue
        }

        userId, ok := doc["UserId"].(string)
        if !ok {
            log.Printf("UserId format not valid: %v", doc["UserId"])
            continue
        }

        if _, exists := validUserIDs[userId]; !exists {
            // Save document to InvalidItemAssignmentData
            _, err := invalidAssignmentCol.InsertOne(ctx, doc)
            if err != nil {
                log.Printf("Insert to InvalidItemAssignmentData failed: %v", err)
            } else {
                log.Printf("🔁 Moved invalid UserId %s to InvalidItemAssignmentData", userId)
            }
        }
    }

    return nil
}

// func main() {
//     if err := godotenv.Load(); err != nil {
//         log.Fatal("Error loading .env file")
//     }

//     mongoURI := os.Getenv("MONGO_URI")
//     dbName := os.Getenv("DB_NAME")
//     mysqlDSN := os.Getenv("MYSQL_DSN")

//     if mongoURI == "" || dbName == "" || mysqlDSN == "" {
//         log.Fatal("Missing required environment variables: MONGO_URI, DB_NAME, MYSQL_DSN")
//     }

//     if err := moveInvalidUserAssignments(mongoURI, dbName, mysqlDSN); err != nil {
//         log.Fatalf("Execution failed: %v", err)
//     }
// }