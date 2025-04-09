package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

var ESClient *elasticsearch.Client
var searchIndex = "test_index"

func main() {
	ESClientConenct()
	ESCreateIndexIfNotExists()

	ESIndexRequest()
}

func ESClientConenct() {
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Elasticsearch bağlantısı başarısız: %s", err)
	}
	ESClient = es
}

func ESCreateIndexIfNotExists() {
	_, err := esapi.IndicesExistsRequest{
		Index: []string{searchIndex},
	}.Do(context.Background(), ESClient)

	if err != nil {
		ESClient.Indices.Create(searchIndex)
	}
}

func ESIndexRequest() {
	document := struct {
		Title   string `json:"title"`
		Content string `json:"content"`
	}{
		Title:   "Test Title",
		Content: "Test Content",
	}

	jsonData, err := json.Marshal(document)
	if err != nil {
		log.Fatalf("JSON oluşturulurken hata oluştu: %s", err)
	}

	res, err := esapi.IndexRequest{
		Index:      searchIndex,
		DocumentID: "1",
		Body:       bytes.NewReader(jsonData),
		Refresh:    "true",
	}.Do(context.Background(), ESClient)

	if err != nil {
		log.Fatalf("Belge ekleme sırasında hata oluştu: %s", err)
	}

	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Belge ekleme sırasında hata oluştu: %s", res.String())
	}

	log.Println("Belge başarıyla eklendi. Indexed document: ", res.String(), " to index: ", searchIndex)

}
