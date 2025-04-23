package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CourseLessonItem struct {
	Id                 string    `bson:"_id"`
	CourseLessonItemId string    `bson:"CourseLessonItemId"`
	LessonId           int       `bson:"LessonId"`
	Title              string    `bson:"Title"`
	Description        string    `bson:"Description"`
	Content            *string   `bson:"Content,omitempty"`
	Time               int       `bson:"Time"`
	VideoUrl           *string   `bson:"VideoUrl,omitempty"`
	Type               int       `bson:"Type"`
	RefId              string    `bson:"RefId"`
	Order              int       `bson:"Order"`
	IsPublished        bool      `bson:"IsPublished"`
	QuestionIds        *string   `bson:"QuestionIds,omitempty"`
	MaxSubmitCount     *int      `bson:"MaxSubmitCount,omitempty"`
	CreatedDate        time.Time `bson:"CreatedDate"`
	ModifiedDate       time.Time `bson:"ModifiedDate"`
	CreatedBy          string    `bson:"CreatedBy"`
	ModifiedBy         string    `bson:"ModifiedBy"`
	OldId              int       `bson:"OldId"`
	IsDeleted          bool      `bson:"IsDeleted"`
	TenantId           int       `bson:"TenantId"`
}

func migrateCourseLessonItems() error {
	mysqlDSN := "docker-mysql:123qwe@tcp(127.0.0.1:3306)/lms"
	mysqlDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return fmt.Errorf("MySQL connection error: %v", err)
	}
	defer mysqlDB.Close()

	mongoURI := "mongodb://localhost:27017"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("MongoDB connection error: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	collection := mongoClient.Database("lms").Collection("NewCourseLessonItem")

	dataCollection := mongoClient.Database("lms").Collection("ItemAssignmentData")

	query := `SELECT 
        LessonId, Title, Description, Content, Time, VideoUrl, Type, RefId, 
        ` + "`Order`" + `, IsPublished, QuestionIds, MaxSubmitCount, TenantId, IsDeleted,
        Created, LastModified, CreatedBy, LastModifiedBy, Id as OldId
        FROM CourseLessonItems`

	rows, err := mysqlDB.Query(query)
	if err != nil {
		return fmt.Errorf("MySQL query error: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item CourseLessonItem
		var content, videoUrl, questionIds sql.NullString
		var maxSubmitCount sql.NullInt64
		var createdStr, lastModifiedStr sql.NullString
		var createdBy, lastModifiedBy sql.NullString
		var oldId int

		err := rows.Scan(
			&item.LessonId,
			&item.Title,
			&item.Description,
			&content,
			&item.Time,
			&videoUrl,
			&item.Type,
			&item.RefId,
			&item.Order,
			&item.IsPublished,
			&questionIds,
			&maxSubmitCount,
			&item.TenantId,
			&item.IsDeleted,
			&createdStr,
			&lastModifiedStr,
			&createdBy,
			&lastModifiedBy,
			&oldId,
		)
		if err != nil {
			return fmt.Errorf("row scan error: %v", err)
		}

		item.Id = primitive.NewObjectID().Hex()
		item.CourseLessonItemId = uuid.New().String()
		// lưu id ban đầu do chuyển id từ int sang GUID, dùng để chuyển các bảng liên quan
		item.OldId = oldId

		if content.Valid {
			item.Content = &content.String
		}
		if videoUrl.Valid {
			item.VideoUrl = &videoUrl.String
		}
		if questionIds.Valid {
			item.QuestionIds = &questionIds.String
		}
		if maxSubmitCount.Valid {
			val := int(maxSubmitCount.Int64)
			item.MaxSubmitCount = &val
		}

		if createdStr.Valid {
			createdTime, err := time.Parse("2006-01-02 15:04:05", createdStr.String)
			if err != nil {
				return fmt.Errorf("error parsing created date: %v", err)
			}
			item.CreatedDate = createdTime
		}
		if lastModifiedStr.Valid {
			modifiedTime, err := time.Parse("2006-01-02 15:04:05", lastModifiedStr.String)
			if err != nil {
				return fmt.Errorf("error parsing modified date: %v", err)
			}
			item.ModifiedDate = modifiedTime
		}

		if createdBy.Valid {
			item.CreatedBy = createdBy.String
		}
		if lastModifiedBy.Valid {
			item.ModifiedBy = lastModifiedBy.String
		}

		// lưu vào bảng NewCourseLessonItem
		_, err = collection.InsertOne(ctx, item)
		if err != nil {
			return fmt.Errorf("MongoDB insert error: %v", err)
		}

		// cập nhật lại NewLessonItemId trong bảng Transcript
		// updateQuery := "UPDATE Transcript SET NewLessonItemId = ? WHERE LessonItemId = ?"
		// _, err = mysqlDB.Exec(updateQuery, item.CourseLessonItemId, item.OldId)
		// if err != nil {
		// 	return fmt.Errorf("error updating Transcript table: %v", err)
		// }

		// cập nhật lại ItemId trong bảng ItemAssignmentData
		_, err = dataCollection.UpdateMany(ctx, bson.M{
			"ItemId": item.OldId,
		}, bson.M{
			"$set": bson.M{
				"OldItemId": item.OldId,
				"NewItemId": item.CourseLessonItemId,
			},
		})
		if err != nil {
			return fmt.Errorf("error updating ItemAssignmentData table: %v", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %v", err)
	}

	log.Println("✅ Migration completed successfully.")
	return nil
}

// optimized version
const (
	mysqlDSN = "docker-mysql:123qwe@tcp(127.0.0.1:3306)/lms"
	mongoURI = "mongodb://localhost:27017"
	dateLayout = "2006-01-02 15:04:05"
)

func MigrateCourseLessonItems() error {
	mysqlDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return fmt.Errorf("MySQL connection error: %v", err)
	}
	defer mysqlDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("MongoDB connection error: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	db := mongoClient.Database("lms")
	collection := db.Collection("NewCourseLessonItem")
	dataCollection := db.Collection("ItemAssignmentData")

	query := `SELECT 
		LessonId, Title, Description, Content, Time, VideoUrl, Type, RefId,
		` + "`Order`" + `, IsPublished, QuestionIds, MaxSubmitCount, TenantId, IsDeleted,
		Created, LastModified, CreatedBy, LastModifiedBy, Id as OldId
		FROM CourseLessonItems`

	rows, err := mysqlDB.Query(query)
	if err != nil {
		return fmt.Errorf("MySQL query error: %v", err)
	}
	defer rows.Close()

	var items []interface{}
	batchSize := 100

	for rows.Next() {
		item, err := scanRow(rows)
		if err != nil {
			return err
		}

		items = append(items, item)

		if len(items) >= batchSize {
			if err := processBatch(ctx, items, collection, dataCollection, mysqlDB); err != nil {
				return err
			}
			items = items[:0]
		}
	}

	if len(items) > 0 {
		if err := processBatch(ctx, items, collection, dataCollection, mysqlDB); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %v", err)
	}

	log.Println("✅ Migration completed successfully.")
	return nil
}

func scanRow(rows *sql.Rows) (CourseLessonItem, error) {
	var item CourseLessonItem
	var content, videoUrl, questionIds sql.NullString
	var maxSubmitCount sql.NullInt64
	var createdStr, lastModifiedStr sql.NullString
	var createdBy, lastModifiedBy sql.NullString
	var oldId int

	err := rows.Scan(
		&item.LessonId, &item.Title, &item.Description, &content,
		&item.Time, &videoUrl, &item.Type, &item.RefId,
		&item.Order, &item.IsPublished, &questionIds, &maxSubmitCount,
		&item.TenantId, &item.IsDeleted, &createdStr, &lastModifiedStr,
		&createdBy, &lastModifiedBy, &oldId,
	)
	if err != nil {
		return item, fmt.Errorf("row scan error: %v", err)
	}

	item.Id = primitive.NewObjectID().Hex()
	item.CourseLessonItemId = uuid.New().String()
	item.OldId = oldId

	if content.Valid {
		item.Content = &content.String
	}
	if videoUrl.Valid {
		item.VideoUrl = &videoUrl.String
	}
	if questionIds.Valid {
		item.QuestionIds = &questionIds.String
	}
	if maxSubmitCount.Valid {
		val := int(maxSubmitCount.Int64)
		item.MaxSubmitCount = &val
	}

	if createdStr.Valid {
		if createdTime, err := time.Parse(dateLayout, createdStr.String); err == nil {
			item.CreatedDate = createdTime
		}
	}
	if lastModifiedStr.Valid {
		if modifiedTime, err := time.Parse(dateLayout, lastModifiedStr.String); err == nil {
			item.ModifiedDate = modifiedTime
		}
	}

	if createdBy.Valid {
		item.CreatedBy = createdBy.String
	}
	if lastModifiedBy.Valid {
		item.ModifiedBy = lastModifiedBy.String
	}

	return item, nil
}

func processBatch(ctx context.Context, items []interface{}, collection *mongo.Collection, dataCollection *mongo.Collection, mysqlDB *sql.DB) error {
	_, err := collection.InsertMany(ctx, items)
	if err != nil {
		return fmt.Errorf("MongoDB bulk insert error: %v", err)
	}

	for _, item := range items {
		courseLessonItem := item.(CourseLessonItem)
		
		// Update Transcript table
		// if _, err := mysqlDB.Exec("UPDATE Transcript SET NewLessonItemId = ? WHERE LessonItemId = ?",
		// 	courseLessonItem.CourseLessonItemId, courseLessonItem.OldId); err != nil {
		// 	return fmt.Errorf("error updating Transcript table: %v", err)
		// }

		// Update ItemAssignmentData
		_, err = dataCollection.UpdateMany(ctx,
			bson.M{"ItemId": courseLessonItem.OldId},
			bson.M{"$set": bson.M{
				// "OldItemId": courseLessonItem.OldId,
				"NewItemId":    courseLessonItem.CourseLessonItemId,
			}},
		)
		if err != nil {
			return fmt.Errorf("error updating ItemAssignmentData: %v", err)
		}
	}

	return nil
}

func main() {
	if err := migrateCourseLessonItems(); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
}
