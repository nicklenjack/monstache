package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	table    string = "tb_album_item_search_"
	mongoUrl string = "mongodb://mongouser:truedian#123@10.0.3.16:27017/db7?authSource=admin"
)

func init() {

	//log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	logFile, err := os.OpenFile("es-check.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Panic("打开日志文件异常")
	}
	log.SetOutput(logFile)
}

var esclient *elastic.Client
var err error
var checked uint64 = 0

func init() {
	esclient, err = elastic.NewClient(elastic.SetURL("http://10.10.42.11:9200"), elastic.SetBasicAuth("elastic", "pdGbLWmJ355GCAp"), elastic.SetSniff(false))
	if err != nil {
		fmt.Println("client err", err)
	}
}

var pageIndex int64 = 0
var lastItemId = "I201805140840532540036278"
var flag = true

const (
	pageSize int64 = 100
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
	log.Println("check finished!!!!", checked)
}

func Find(lastItemId string) (results []bson.D, err error) {
	ctx, cannel := context.WithTimeout(context.Background(), time.Minute)
	defer cannel()
	filter := bson.D{{"itemId", bson.D{{"$gt", lastItemId}}}}
	//filter := bson.D{}
	opts := options.Find().SetLimit(pageSize).SetSort(bson.D{{"itemId", 1}}).SetProjection(bson.D{{"itemId", 1}, {"sync", 1}, {"albumId", 1}})
	cur, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	err = cur.All(context.Background(), &results)
	return
}

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
		return false, ""
	}

	returnLastItemId = results[len(results)-1].Map()["itemId"].(string)
	if err != nil {
		log.Println("mongoPageQuery Find", err)
	}
	//start1 := time.Now() // 获取当前时间
	//fmt.Print("results size:", len(results))
	checkEs(results)
	elapsed := time.Since(start)
	fmt.Println("es-check执行完成耗时：", elapsed, lastItemId, pageIndex)
	//elapsed1 := time.Since(start1)
	//fmt.Println("检查mongodb数据完成耗时：", elapsed1)
	return true, returnLastItemId
}

func batchEsCheck(results []primitive.D) {
	ctx := context.Background()
	multiGet := esclient.MultiGet()

	for _, result := range results {
		albumId := result.Map()["albumId"].(string)
		itemId := result.Map()["itemId"].(string)
		var len = len([]rune(albumId))
		var dbKey = albumId[len-3 : len-2]
		var instanceKey = albumId[len-1 : len]
		var index = instanceKey + dbKey

		id := albumId + "-" + itemId
		get := elastic.NewMultiGetItem(). // 通过NewMultiGetItem配置查询条件
							Index(index). // 设置索引名
							Routing(albumId).
							Id(id)
		// FetchSource(&fetchSourceContext)
		multiGet.Add(get)
	}

	res, err := multiGet.Do(ctx)
	if err != nil {
		log.Println(err)
	} else {
		size := len(res.Docs)
		log.Println(size)
		//size := len(res.Docs)
		//if size == pageSize {
		//	atomic.AddUint64(&checked, uint64(pageSize))
		//	if checked%10000 == 0 {
		//		fmt.Println("checkEs checked:", checked)
		//	}
		//} else {
		//	for _, doc := range res.Docs {
		//		fmt.Println("doc", doc.Found)
		//		var content map[string]interface{}
		//		tmp, _ := doc.Source.MarshalJSON()
		//		err := json.Unmarshal(tmp, &content)
		//		if err != nil {
		//			panic(err)
		//		}
		//
		//		// fmt.Println(content["albumId"].(string), content["itemId"].(string))
		//	}
		//}
	}
}

func checkEs(results []primitive.D) {
	var wg sync.WaitGroup
	for _, result := range results {
		wg.Add(1)
		go func() {
			albumId := result.Map()["albumId"].(string)
			itemId := result.Map()["itemId"].(string)
			var len = len([]rune(albumId))
			var dbKey = albumId[len-3 : len-2]
			var instanceKey = albumId[len-1 : len]
			var index = instanceKey + dbKey

			ctx := context.Background()

			/**
			  通过id根据条件查询es，是否es中存在该数据，不存在打印出来
			*/
			//termQuery := elastic.NewTermQuery("itemId", doc["itemId"])
			//id := albumId + "-" + itemId
			//get, err := esclient.Get().Index(table + index).Routing(albumId).Id(id).Do(ctx)
			//
			//if err != nil {
			//	log.Println("Process error, doc albumId:", albumId, "itemId:", itemId, err)
			//	return
			//}
			//if get.Found {
			//	atomic.AddUint64(&checked, 1)
			//
			//	if checked%100000 == 0 {
			//		log.Println("checkEs checked:", checked, " pageIndex", pageIndex)
			//	}
			//} else {
			//	log.Println("checkEs albumId:", albumId, "itemId:", albumId, "NOT FOOUND!!!")
			//}

			/*
				根据条件查询es，是否es中存在该数据，不存在打印出来
			**/
			termQuery := elastic.NewTermQuery("itemId", itemId)
			searchResult, err := esclient.Search().Index(table + index).Routing(albumId).Query(termQuery).Do(ctx) // 设置查询条件
			if err != nil {
				log.Println("Process error, doc itemId:", itemId, err)
				return
			}
			if searchResult.TotalHits() == 0 {
				log.Println("'" + albumId + "','" + itemId + "','loss'")
			} else {
				atomic.AddUint64(&checked, 1)
				//if checked%100000 == 0 {
				//	log.Println("'checked','" + checked + "','pageIndex','" + pageIndex + "'")
				//}
			}
			if searchResult.TotalHits() > 1 {
				log.Println("'" + albumId + "','" + itemId + "','dup'")
			}
			defer wg.Done()
		}()
	}
	wg.Wait()
}
