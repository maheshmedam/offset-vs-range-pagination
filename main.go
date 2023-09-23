package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func initMongoClient(ctx context.Context) *mongo.Client {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017/")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

type Task struct {
	Name string
}

func createTask(ctx context.Context, collection *mongo.Collection, task *Task, N int) {
	for i := 0; i < N; i++ {
		collection.InsertOne(ctx, task)
	}
}

// offsetBasedPageIterator iterates till end of collection using offsets.
func offsetBasedPageIterator(ctx context.Context, collection *mongo.Collection, pageSize int, total int) {
	fmt.Println("=================")
	fmt.Println("Using Offset based page iterator")
	fmt.Println("=================")
	pageNums := []int{}
	for i := 0; i < total/pageSize; i++ {
		pageNums = append(pageNums, i)
	}
	for pageNum := range pageNums {
		pageFetchStartTime := time.Now()
		collection.Aggregate(ctx, mongo.Pipeline{
			bson.D{{"$skip", pageNum * pageSize}},
			bson.D{{"$limit", pageSize}},
		})
		pageFetchElapsedTime := time.Since(pageFetchStartTime).Nanoseconds()
		fmt.Println(pageFetchElapsedTime)
	}
}

// rangeBasedPageIterator iterates till end pf collection using id as cursor field
func rangeBasedPageIterator(ctx context.Context, collection *mongo.Collection, pageSize int) {
	fmt.Println("=================")
	fmt.Println("Using Range based page iterator")
	fmt.Println("=================")
	var lastId interface{}
	for {
		pageFetchStartTime := time.Now()
		var pipeline mongo.Pipeline
		if lastId == nil {
			pipeline = mongo.Pipeline{
				bson.D{{"$limit", pageSize}},
			}
		} else {
			pipeline = mongo.Pipeline{
				bson.D{{
					"$match", bson.M{
						"_id": bson.M{
							"$gt": lastId,
						},
					},
				}},
				bson.D{
					{"$limit", pageSize},
				},
			}
		}
		cursor, err := collection.Aggregate(ctx, pipeline)
		if err != nil {
			log.Fatalf("error from cursor: %v", err)
		}
		pageFetchElapsedTime := time.Since(pageFetchStartTime).Nanoseconds()
		var results []bson.M
		cursor.All(ctx, &results)
		if len(results) == 0 {
			break
		}
		lastId = results[len(results)-1]["_id"]
		fmt.Println(pageFetchElapsedTime)
	}
}

func main() {
	ctx := context.Background()
	client := initMongoClient(ctx)
	collection := client.Database("tasker").Collection("tasks")
	collection.Drop(ctx)
	totalRecords := 100000
	pageSize := 1000
	createTask(ctx, collection, &Task{
		Name: "something",
	}, totalRecords)
	offsetBasedPageIterator(ctx, collection, pageSize, totalRecords)
	rangeBasedPageIterator(ctx, collection, pageSize)
}
