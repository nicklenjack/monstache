package main

import (
	"crypto/sha1"
	"encoding/hex"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rwynn/monstache/monstachemap"
)

var count uint64 = 0
var delCount uint64 = 0

func init() {

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	logFile, err := os.OpenFile("sync.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Panic("打开日志文件异常")
	}
	log.SetOutput(logFile)
}

// a plugin to convert document values to uppercase
func Map(input *monstachemap.MapperPluginInput) (output *monstachemap.MapperPluginOutput, err error) {

	doc := input.Document
	//fmt.Println("doc: %v", doc)

	defer func() {
		//类型不匹配 打印日志，不退出程序
		if re := recover(); re != nil {
			log.Println(doc["itemId"], "Map error occur", re)
		}
	}()

	atomic.AddUint64(&count, 1)
	//fmt.Println("count:", count)
	output, needDel := doDel(doc, output)
	if needDel {
		atomic.AddUint64(&delCount, 1)
		//fmt.Println("delCount:", delCount)
		return
	}
	output = doNormal(doc, output)
	return
}

//正常数据的同步处理
func doNormal(doc map[string]interface{}, output *monstachemap.MapperPluginOutput) *monstachemap.MapperPluginOutput {
	itemTagList, shareType, imgIdStr := refactorDoc(doc)
	syncDoc := newDoc(doc, itemTagList, shareType, imgIdStr)

	syncDoc["syncTime"] = time.Now()

	output = &monstachemap.MapperPluginOutput{Document: syncDoc}

	setIndex(doc, output)
	return output
}

//需要删除的数据的同步处理
func doDel(doc map[string]interface{}, output *monstachemap.MapperPluginOutput) (*monstachemap.MapperPluginOutput, bool) {
	switch doc["state"].(type) {
	case int32:
		if doc["state"].(int32) == int32(1) {
			output := del(doc, output)
			return output, true
		}
	case float64:
		if doc["state"].(float64) == float64(1) {
			output := del(doc, output)
			return output, true

		}
	default:
		log.Printf("itemId= %v, albumId= %v, Map [[state]] param type is %T \n", doc["itemId"], doc["albumId"], doc["state"])
	}
	//违法的
	if doc["compliance"] != nil {
		compliance := doc["compliance"].(map[string]interface{})
		switch compliance["state"].(type) {
		case int32:
			if compliance["state"].(int32) == int32(3) {
				output := del(doc, output)
				return output, true
			}
		case float64:
			if compliance["state"].(float64) == float64(3) {
				output := del(doc, output)
				return output, true

			}
		default:
			log.Printf("itemId= %v, albumId= %v, Map [[compliance.state]] param type is %T \n", doc["itemId"], doc["albumId"], compliance["state"])
		}
	}

	return output, false
}

func del(doc map[string]interface{}, output *monstachemap.MapperPluginOutput) *monstachemap.MapperPluginOutput {
	delDoc := newDelDoc(doc)
	delDoc["syncTime"] = time.Now()
	output = &monstachemap.MapperPluginOutput{Document: delDoc}
	setIndex(doc, output)
	return output
}

//需要改造文档字段的处理
func refactorDoc(doc map[string]interface{}) (string, int8, string) {
	var itemTagList string
	if doc["itemTagList"] != nil {
		strArray := getStrArr(doc["itemTagList"].([]interface{}))
		itemTagList = strings.Join(strArray, "-")

	}

	var shareType int8 = 0
	if doc["shareHistory"] != nil && len(doc["shareHistory"].(map[string]interface{})) != 0 {
		shareType = 1
	}

	var imgIdStr string
	if doc["itemPhotoList"] != nil && len(doc["itemPhotoList"].([]interface{})) != 0 {
		var imgIds []string
		for _, param := range doc["itemPhotoList"].([]interface{}) {
			p := param.(map[string]interface{})
			if p["photoQiniuUrl"] != nil {
				photoQiniuUrl := p["photoQiniuUrl"].(string)
				path := getPath(photoQiniuUrl)
				imgId := Sha1(path)
				imgIds = append(imgIds, imgId)
			}
		}
		imgIdStr = strings.Join(imgIds, "-")

	}
	return itemTagList, shareType, imgIdStr
}

//设置索引，ID，路径等信息
func setIndex(doc map[string]interface{}, output *monstachemap.MapperPluginOutput) {
	partitionKey := doc["albumId"].(string)
	var len = len([]rune(partitionKey))
	var dbKey = partitionKey[len-3 : len-2]

	var instanceKey = partitionKey[len-1 : len]

	var index = instanceKey + dbKey
	//fmt.Println("index", index)

	output.Index = "tb_album_item_search_" + index

	var id = doc["albumId"].(string) + "-" + doc["itemId"].(string)
	output.ID = id
	output.Routing = doc["albumId"].(string)
}

//生成用来删除的doc
func newDelDoc(doc map[string]interface{}) map[string]interface{} {

	syncDoc := make(map[string]interface{}, 30)

	if doc["compliance"] != nil {
		syncDoc["compliance"] = doc["compliance"]
	}

	if doc["state"] != nil {
		syncDoc["state"] = doc["state"]
	}

	syncDoc["albumId"] = doc["albumId"]
	syncDoc["createTime"] = nil
	syncDoc["digitalWatermarkId"] = nil
	syncDoc["goodsNum"] = nil
	syncDoc["itemCaption"] = nil
	syncDoc["itemId"] = doc["itemId"]
	syncDoc["itemName"] = nil
	syncDoc["itemNameWord"] = nil
	syncDoc["itemOwnSrcId"] = nil
	syncDoc["itemTagList"] = nil
	syncDoc["itemType"] = nil
	syncDoc["markCode"] = nil
	syncDoc["newsendTime"] = nil
	syncDoc["personal"] = nil
	syncDoc["parentAlbumId"] = nil
	syncDoc["itemType"] = nil
	syncDoc["updateTime"] = nil
	syncDoc["distributionAgentMark"] = nil
	syncDoc["distributionMark"] = nil
	syncDoc["shareType"] = nil
	syncDoc["imgIds"] = nil
	return syncDoc
}

//生成一个正常同步的doc
func newDoc(doc map[string]interface{}, itemTagList string, shareType int8, imgIdStr string) map[string]interface{} {
	syncDoc := make(map[string]interface{}, 30)
	if doc["albumId"] != nil {
		syncDoc["albumId"] = doc["albumId"]
	}

	if doc["compliance"] != nil {
		syncDoc["compliance"] = doc["compliance"]
	}

	if doc["createTime"] != nil {
		syncDoc["createTime"] = doc["createTime"].(time.Time).UnixMilli()
	}

	if doc["digitalWatermarkId"] != nil {
		syncDoc["digitalWatermarkId"] = doc["digitalWatermarkId"]
	}

	if doc["goodsNum"] != nil {
		syncDoc["goodsNum"] = doc["goodsNum"]
	}

	if doc["itemCaption"] != nil {
		syncDoc["itemCaption"] = doc["itemCaption"]
	}

	if doc["itemId"] != nil {
		syncDoc["itemId"] = doc["itemId"]
	}

	// if doc["itemName"] != nil {
	// 	syncDoc["itemKeyValue"] = doc["itemName"]
	// }

	if doc["itemName"] != nil {
		syncDoc["itemName"] = doc["itemName"]
	}

	if doc["itemName"] != nil {

		word := strings.Replace(doc["itemName"].(string), " ", "-", -1)
		syncDoc["itemNameWord"] = word
	}

	if doc["itemOwnSrcId"] != nil {
		syncDoc["itemOwnSrcId"] = doc["itemOwnSrcId"]
	}

	syncDoc["itemTagList"] = itemTagList

	if doc["itemType"] != nil {
		syncDoc["itemType"] = doc["itemType"]
	}

	if doc["markCode"] != nil {
		syncDoc["markCode"] = doc["markCode"]
	}

	if doc["newsendTime"] != nil {
		syncDoc["newsendTime"] = doc["newsendTime"].(time.Time).UnixMilli()
	}

	if doc["personal"] != nil {
		syncDoc["personal"] = doc["personal"]
	}

	if doc["parentAlbumId"] != nil {
		syncDoc["parentAlbumId"] = doc["parentAlbumId"]
	}

	if doc["state"] != nil {
		syncDoc["state"] = doc["state"]
	}

	if doc["updateTime"] != nil {
		syncDoc["updateTime"] = doc["updateTime"].(time.Time).UnixMilli()
	}

	if doc["distributionAgentMark"] != nil {
		syncDoc["distributionAgentMark"] = doc["distributionAgentMark"]
	}

	if doc["distributionMark"] != nil {
		syncDoc["distributionMark"] = doc["distributionMark"]
	}

	syncDoc["shareType"] = shareType

	syncDoc["imgIds"] = imgIdStr
	return syncDoc
}

func Sha1(data string) string {
	sha1 := sha1.New()
	sha1.Write([]byte(data))
	return hex.EncodeToString(sha1.Sum([]byte("")))
}

func getPath(url string) string {
	ind := strings.Index(url, "//")
	url = url[ind+2:]
	ind = strings.Index(url, "/")
	url = url[ind+1:]
	return url
}

func getStrArr(params []interface{}) []string {

	strArray := make([]string, len(params))
	for i, param := range params {
		switch v := param.(type) {
		case string:
			strArray[i] = param.(string)
		case int:
			strV := strconv.FormatInt(int64(v), 10)
			strArray[i] = strV
		case int32:
			strV := strconv.FormatInt(int64(v), 10)
			strArray[i] = strV
		case int64:
			strV := strconv.FormatInt(v, 10)
			strArray[i] = strV
		default:
			log.Printf("[[getStrArr]] param type is %T \n", v)
		}
	}
	return strArray
}
