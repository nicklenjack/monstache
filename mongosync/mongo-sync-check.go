package main

import (
	"context"
	"fmt"
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
	logFile, err := os.OpenFile("mongo-sync-check.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Panic("打开日志文件异常")
	}
	log.SetOutput(logFile)
}

var pageIndex int64 = 0
var lastItemId string = ""
var flag bool = true

const (
	pageSize int64 = 1000
)

func main() {
	for {
		//fmt.Println("sync lastItemId:", lastItemId)
		flag, lastItemId = mongoPageQuery(lastItemId)
		if !flag {
			break
		}
		pageIndex++
	}
	log.Println("not sync:", nosync)
	log.Println("synced:", synced)
	log.Println("query finished!!!!")
}

func Find(lastItemId string) (results []bson.D, err error) {
	ctx, cannel := context.WithTimeout(context.Background(), time.Minute)
	defer cannel()
	filter := bson.D{{"itemId", bson.D{{"$gt", lastItemId}}}}
	//filter := bson.D{}
	opts := options.Find().SetLimit(pageSize).SetSort(bson.D{{"itemId", 1}}).
		SetProjection(bson.D{{"itemId", 1}, {"sync", 1}, {"albumId", 1}})
	cur, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	err = cur.All(context.Background(), &results)
	return
}

var synced uint64 = 0
var nosync uint64 = 0

var mongoclient *mongo.Client
var conerr error
var collection *mongo.Collection

func init() {
	clientOptions := options.Client().ApplyURI(mongoUrl)
	mongoclient, conerr = mongo.Connect(context.TODO(), clientOptions)
	collection = mongoclient.Database("db7").Collection("tb_album_item")
	if conerr != nil {
		log.Fatal("mongoPageQuery Connect", conerr)
	}
}

func mongoPageQuery(lastItemId string) (flag bool, returnLastItemId string) {
	start := time.Now() // 获取当前时间
	results, err := Find(lastItemId)
	if int64(len(results)) < pageSize {
		log.Println("size", int64(len(results)), "pageIndex", pageIndex)
		return false, ""
	}
	elapsed := time.Since(start)

	fmt.Println("查询mongodb执行完成耗时：", elapsed)
	returnLastItemId = results[len(results)-1].Map()["itemId"].(string)
	if err != nil {
		log.Println("mongoPageQuery Find", err)
	}

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
	return true, returnLastItemId
}
