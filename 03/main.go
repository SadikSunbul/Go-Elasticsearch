package main

import (
	"context"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/create"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
)

func main() {
	// Elasticsearch'e bağlan
	client, err := ConnectToElasticsearch()
	if err != nil {
		log.Fatalf("Bağlantı hatası: %v", err)
	}

	// İndeks oluştur
	CreateIndex(client)

	// Belge indeksle
	IndexDocument(client)

	// Belgeyi al
	GetDocument(client)

	// Belgenin varlığını kontrol et
	CheckDocumentExists(client)

	// Arama yap
	SearchWithMatchQuery(client)

	// Toplama (aggregation) yap
	AggregatePrices(client)
}

// Elasticsearch'e bağlanma fonksiyonu
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

// İndeks oluşturma fonksiyonu
func CreateIndex(client *elasticsearch.TypedClient) {
	res, err := client.Indices.Create("test-index").
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"price": types.NewIntegerNumberProperty(),
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		log.Fatalf("İndeks oluşturma hatası: %v", err)
	}
	fmt.Println("İndeks oluşturma başarılı:", res)
}

// Belge indeksleme fonksiyonu
func IndexDocument(client *elasticsearch.TypedClient) {
	// Struct ile belge
	document := struct {
		ID    int    `json:"id"`
		Name  string `json:"name"`
		Price int    `json:"price"`
	}{
		ID:    1,
		Name:  "Foo",
		Price: 10,
	}

	res, err := client.Index("test-index").
		Request(document).
		Refresh(refresh.True). // İndeksleme sonrası hemen yenile
		Do(context.Background())
	if err != nil {
		log.Fatalf("Belge indeksleme hatası: %v", err)
	}
	fmt.Println("Belge indeksleme başarılı:", res)
}

// Belge alma fonksiyonu
func GetDocument(client *elasticsearch.TypedClient) {
	res, err := client.Get("test-index", "1"). // ID otomatik oluşturulabilir, burada "1" varsayıyoruz
							Do(context.Background())
	if err != nil {
		log.Fatalf("Belge alma hatası: %v", err)
	}
	if res.Found {
		fmt.Println("Belge bulundu, içerik:", res.Source_)
	} else {
		fmt.Println("Belge bulunamadı.")
	}
}

// Belgenin varlığını kontrol etme fonksiyonu
func CheckDocumentExists(client *elasticsearch.TypedClient) {
	exists, err := client.Core.Exists("test-index", "1").
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

// Match sorgusu ile arama fonksiyonu
func SearchWithMatchQuery(client *elasticsearch.TypedClient) {
	res, err := client.Search().
		Index("test-index").
		Request(&search.Request{
			Query: &types.Query{
				Match: map[string]types.MatchQuery{
					"name": {Query: "Foo"},
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		log.Fatalf("Arama hatası: %v", err)
	}

	fmt.Println("Arama başarılı, sonuç sayısı:", res.Hits.Total.Value)
	for _, hit := range res.Hits.Hits {
		fmt.Printf("ID: %s, Score: %f\n", hit.Id_, hit.Score_)
	}
}

// Toplama (aggregation) fonksiyonu
func AggregatePrices(client *elasticsearch.TypedClient) {
	size := 0
	field := "price"
	res, err := client.Search().
		Index("test-index").
		Request(&search.Request{
			Size: &size,
			Aggregations: map[string]types.Aggregations{
				"total_prices": {
					Sum: &types.SumAggregation{
						Field: &field,
					},
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		log.Fatalf("Toplama hatası: %v", err)
	}

	if sum, ok := res.Aggregations["total_prices"].(*types.SumAggregate); ok {
		fmt.Println("Toplam fiyat:", sum.Value)
	}
}
