package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

func main() {
	client, err := ConnectToElasticsearch()
	if err != nil {
		log.Fatalf("Bağlantı hatası: %v", err)
	}

	CreateDocument(client)
	SearchDocuments(client)
	CheckDocumentExists(client)
	SearchWithTermQuery(client) // Yeni fonksiyonu ekledik
}

func ConnectToElasticsearch() (*elasticsearch.TypedClient, error) {
	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	})
	if err != nil {
		log.Fatalf("İstemci bağlantı hatası: %v", err)
	}

	info, err := client.Info().Do(context.Background())
	if err != nil {
		log.Fatalf("Bilgi alma hatası: %v", err)
	}
	fmt.Println("Bağlantı başarılı, info:", info)

	return client, nil
}

func CreateDocument(client *elasticsearch.TypedClient) {
	jsonBody := `{"name": "Foo", "language": "Go"}` // "Foo" ile eşleşecek
	res, err := client.Create("my_index", "my_doc_id").
		Raw(strings.NewReader(jsonBody)).
		Do(context.Background())
	if err != nil {
		log.Fatalf("Belge oluşturma hatası: %v", err)
	}
	fmt.Println("Belge oluşturma başarılı:", res)
}

func SearchDocuments(client *elasticsearch.TypedClient) {
	res, err := client.Search().
		Index("my_index").
		AllowPartialSearchResults(true).
		Do(context.Background())
	if err != nil {
		log.Fatalf("Arama hatası: %v", err)
	}
	fmt.Println("Arama başarılı:", res)
}

func CheckDocumentExists(client *elasticsearch.TypedClient) {
	exists, err := client.Core.Exists("my_index", "my_doc_id").
		IsSuccess(context.Background())
	if err != nil {
		log.Fatalf("Varlık kontrol hatası: %v", err)
	}
	if exists {
		fmt.Println("Belge mevcut!")
	} else {
		fmt.Println("Belge bulunamadı.")
	}
}

func SearchWithTermQuery(client *elasticsearch.TypedClient) {
	// Term sorgusu oluştur
	query := types.Query{
		Term: map[string]types.TermQuery{
			"name": {Value: "Foo"}, // "name" alanında "Foo" arar
		},
	}

	// Sorguyu JSON'a çevir
	queryJSON, err := json.Marshal(query)
	if err != nil {
		log.Fatalf("Sorgu JSON'a çevirme hatası: %v", err)
	}

	// Arama isteği yap
	res, err := client.Search().
		Index("my_index").
		Raw(strings.NewReader(string(queryJSON))).
		Do(context.Background())
	if err != nil {
		log.Fatalf("Arama hatası: %v", err)
	}

	// Yanıtın durumunu kontrol et
	if res.Hits.Total.Value == 0 {
		log.Fatalf("Arama isteği başarısız: Sonuç bulunamadı")
	}

	fmt.Println("Arama başarılı, sonuç sayısı:", res.Hits.Total.Value)
	for _, hit := range res.Hits.Hits {
		fmt.Printf("ID: %s, Score: %f\n", hit.Id_, hit.Score_)
	}
}
