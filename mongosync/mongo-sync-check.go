package main

import (
	"context"
	"log"
	"os"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoUrl = "mongodb://mongouser:truedian#123@10.0.3.16:27017/db7?authSource=admin"

func init() {

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	logFile, err := os.OpenFile("mongo-sync-check.log", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Panic("打开日志文件异常")
	}
	log.SetOutput(logFile)
}

var pageIndex int64 = 0

func main() {
	for {
		results := mongoPageQuery(pageIndex)
		if len(results) <= 0 {
			break
		}
		pageIndex++
	}
	log.Println("not sync:", nosync)
	log.Println("synced:", synced)
	log.Println("query finished!!!!")
}

func Find(database *mongo.Database, collection string, limit, index int64) (results []bson.D, err error) {
	ctx, cannel := context.WithTimeout(context.Background(), time.Minute)
	defer cannel()

	opts := options.Find().SetSkip(index * limit).SetLimit(limit)
	cur, err := database.Collection(collection).Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	err = cur.All(context.Background(), &results)
	return
}

var synced uint64 = 0
var nosync uint64 = 0

func mongoPageQuery(index int64) (results []bson.D) {
	clientOptions := options.Client().ApplyURI(mongoUrl)

	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal("mongoPageQuery Connect", err)
	}

	database := client.Database("db7")
	results, err = Find(database, "tb_album_item", 1000, index)
	if err != nil {
		log.Println("mongoPageQuery Find", err)
	}
	//fmt.Print("results size:", len(results))
	for _, result := range results {
		if result.Map()["sync"] == int32(1) {
			atomic.AddUint64(&synced, 1)
			if synced%100000 == 0 {
				log.Println("mongosync synced!!!", synced, "pageIndex", pageIndex)
			}
		} else {
			atomic.AddUint64(&nosync, 1)
			log.Println(result.Map()["albumId"].(string), result.Map()["itemId"].(string), nosync)
		}
	}
	return
}
