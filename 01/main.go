package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"log"
	"strings"
)

func main() {

	client, err := ConnectElasticsearch()

	if err != nil {
		log.Fatalf("Elasticsearch bağlantı hatası: %v", err)
	}
	res, err := client.Info()

	if err != nil {
		log.Fatalf("Bilgi alma hatası: %v", err)
	}

	fmt.Println(res)

	fmt.Println("*----------------------------------*")
	fmt.Println()
	// CreateIndex(client)

	// IndexingDocuments(client)

	// GettingDocuments(client)

	// SearchingDocuments(client)

	// UpdatingDocuments(client)

	// SearchingDocumentsV2(client)

	// DeletingDocument(client)

	DeletingAnIndex(client)
}

func ConnectElasticsearch() (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	}
	es, err := elasticsearch.NewClient(cfg)

	return es, err
}

func CreateIndex(client *elasticsearch.Client) {
	a, err := client.Indices.Create("my_index")
	if err != nil {
		log.Fatalf("my_index is not created error:", err)
	}

	fmt.Println("my_index:", a)
}

func IndexingDocuments(client *elasticsearch.Client) {
	document := struct {
		Name string `json:"name"`
	}{
		"go-elasticsearch",
	}
	data, _ := json.Marshal(document)
	d, err := client.Index("my_index2", bytes.NewReader(data)) // buradakı ındex yok ısede otomatık bır sekıdle olusturuyor.
	if err != nil {
		log.Fatalf("indexinf documents is error :", err)
	}

	fmt.Println("indexinf documents is succesc:", d)
}

func GettingDocuments(client *elasticsearch.Client) {
	d, err := client.Get("my_index", "IrVsBZYBYHowjX76PwBi")
	if err != nil {
		log.Fatalf("Not getting Document is err:", err)
	}

	fmt.Println("Getting Documents is succes :", d)
}

func SearchingDocuments(client *elasticsearch.Client) {
	query := `{ "query": { "match_all": {} } }`
	d, err := client.Search(
		client.Search.WithIndex("my_index"),
		client.Search.WithBody(strings.NewReader(query)),
	)

	if err != nil {
		log.Fatalf("Searchşng documents is err:", err)
	}

	fmt.Println("Searching documents is success:", d)
}

func SearchingDocumentsV2(client *elasticsearch.Client) {
	// Sorguyu map ile tanımla
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{}, // Boş bir map, çünkü match_all parametre almıyor
		},
	}

	// Map'i JSON'a çevir
	data, err := json.Marshal(query)
	if err != nil {
		log.Fatalf("JSON marshaling failed: %v", err)
	}

	// Arama isteğini yap
	d, err := client.Search(
		client.Search.WithIndex("my_index"),
		client.Search.WithBody(bytes.NewReader(data)), // strings.NewReader yerine bytes.NewReader da kullanılabilir
	)
	if err != nil {
		log.Fatalf("Searching documents is err: %v", err)
	}

	fmt.Println("Searching documents is success:", d)
}

func UpdatingDocuments(client *elasticsearch.Client) {
	d, err := client.Update("my_index", "IrVsBZYBYHowjX76PwBi", strings.NewReader(`{"doc": {"language": "Go"}}`))
	if err != nil {
		log.Fatalf("Update Documents is err:", err)
	}

	fmt.Println("Update Documents is successful:", d)
}

func DeletingDocument(client *elasticsearch.Client) {
	d, err := client.Delete("my_index", "IrVsBZYBYHowjX76PwBi")
	if err != nil {
		log.Fatalf("Deleting Document is err:", err)
	}
	fmt.Println("Delting Documetn is successful:", d)
}

func DeletingAnIndex(client *elasticsearch.Client) {
	d, err := client.Indices.Delete([]string{"my_index"})
	if err != nil {
		log.Fatalf("Deleting an index is err:", err)
	}
	fmt.Println("Deleting sn index is successfull:", d)
}
