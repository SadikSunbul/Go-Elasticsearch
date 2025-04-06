package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/create"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

func ConnectToElasticsearch() (*elasticsearch.TypedClient, error) {
	return elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	})
}

func main() {
	es, err := ConnectToElasticsearch()
	if err != nil {
		log.Fatal("connect to eleasticsearch is err:", err)
	}

	fmt.Println("connect info:", es.Info())

	fmt.Println("*......Connec to Elasticsearch is success......*")

	//DeleteIndex(es, "test_1")
	//CreateIndex(es, "test_1")
	//CreateDocument(es, "test_1")
	//CreateMultipleDocument(es, "test_1")
	// PrintMapping(es, "test_1")
	//CreateBookIndex(es, "book_index")
	CreateBookDocument(es, "book_index")
}

func CreateIndex(es *elasticsearch.TypedClient, indexName string) {
	respons, err := es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"price": types.NewIntegerNumberProperty(),
				},
			},
			Settings: &types.IndexSettings{
				NumberOfShards:   "3",
				NumberOfReplicas: "2",
			},
		}).
		Do(context.Background())

	if err != nil {
		log.Fatalf("erorr create index : ", err)
	}
	fmt.Println("response:", respons)
}

// İndeks silme fonksiyonu
func DeleteIndex(es *elasticsearch.TypedClient, indexName string) {
	// İndeksi sil, eğer yoksa hata verme
	_, err := es.Indices.Delete(indexName).
		IgnoreUnavailable(true).
		Do(context.Background())

	if err != nil {
		log.Fatalf("İndeks silme hatası: %v", err)
	}
	fmt.Printf("%s indeksi başarıyla silindi\n", indexName)
}

func CreateDocument(es *elasticsearch.TypedClient, indexName string) {
	document := map[string]interface{}{
		"name":  "John Doe",
		"age":   30,
		"city":  "New York",
		"price": "100",
	}

	respons, err := es.Index(indexName).
		Request(document).
		Do(context.Background())

	if err != nil {
		log.Fatalf("document create error: ", err)
	}
	fmt.Println("document result: ", respons.Result)
	fmt.Println("document shards: ", respons.Shards_)
	fmt.Println("document id: ", respons.Id_)
	fmt.Println("document index: ", respons.Index_)

}

func CreateMultipleDocument(es *elasticsearch.TypedClient, indexName string) {
	documents := []map[string]interface{}{
		{
			"name": "John Doe",
			"age":  30,
			"city": "New York",
		},
		{
			"name": "Jane Doe",
			"age":  25,
			"city": "Los Angeles",
		},
	}

	for _, doc := range documents {
		_, err := es.Index(indexName).
			Request(doc).
			Do(context.Background())

		if err != nil {
			log.Fatalf("Döküman oluşturma hatası: %v", err)
		}
	}
	fmt.Println("Tüm dökümanlar başarıyla oluşturuldu")
}

func PrintMapping(es *elasticsearch.TypedClient, indexName string) {
	respons, err := es.Indices.GetMapping().Index(indexName).
		AllowNoIndices(true).
		Do(context.Background())

	if err != nil {
		log.Fatalf("mapping get error: ", err)
	}

	// Mapping bilgilerini JSON olarak yazdır
	mappingJSON, err := json.MarshalIndent(respons, "", "  ")
	if err != nil {
		log.Fatalf("JSON dönüşüm hatası: %v", err)
	}

	fmt.Printf("İndeks '%s' için mapping bilgileri:\n%s\n", indexName, string(mappingJSON))
}

func CreateBookIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce varolan indeksi sil
	_, err := es.Indices.Delete(indexName).
		IgnoreUnavailable(true).
		Do(context.Background())

	if err != nil {
		log.Fatalf("İndeks silme hatası: %v", err)
	}

	// Yeni indeksi oluştur
	respons, err := es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"book_reference": types.NewKeywordProperty(),
					"price":          types.NewFloatNumberProperty(),
					"publish_date":   types.NewDateProperty(),
					"is_available":   types.NewBooleanProperty(),
				},
			},
		}).
		Do(context.Background())

	if err != nil {
		log.Fatalf("İndeks oluşturma hatası: %v", err)
	}
	fmt.Printf("İndeks başarıyla oluşturuldu: %v\n", respons)
}

func CreateBookDocument(es *elasticsearch.TypedClient, indexName string) {
	documents := []map[string]interface{}{
		{
			"book_reference": "1234567890",
			"price":          100.0,
			"publish_date":   "2023-01-01",
			"is_available":   true,
		},
		{
			"book_reference": "1234567890",
			"price":          100.0,
			"publish_date":   "2023-01-01",
			"is_available":   true,
		},
	}

	for _, doc := range documents {
		_, err := es.Index(indexName).
			Request(doc).
			Do(context.Background())

		if err != nil {
			log.Fatalf("Döküman oluşturma hatası: %v", err)
		}
	}
	fmt.Println("Tüm dökümanlar başarıyla oluşturuldu")
}
