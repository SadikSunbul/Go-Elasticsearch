package main

import (
	"context"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
)

func ConnectToElasticsearch() (*elasticsearch.TypedClient, error) {
	return elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	})
}

func main() {
	// Elasticsearch'e bağlan
	es, err := ConnectToElasticsearch()
	if err != nil {
		log.Fatal("Elasticsearch bağlantı hatası:", err)
	}

	// Bağlantı bilgilerini göster
	info, err := es.Info().Do(context.Background())
	if err != nil {
		log.Fatal("Bilgi alma hatası:", err)
	}
	fmt.Printf("Elasticsearch'e bağlandı! info: %+v\n", info)
	ctx := context.Background()
	/*
		// İndeksi sil ve yeniden oluştur
		ctx := context.Background()
		_, err = es.Indices.Delete("my_index").Do(ctx)
		if err != nil {
			log.Printf("İndeks silme hatası: %v", err)
		}

		_, err = es.Indices.Create("my_index").Do(ctx)
		if err != nil {
			log.Fatal("İndeks oluşturma hatası:", err)
		}

		// Manuel olarak belgeleri ekle
		documents := []map[string]interface{}{
			{
				"title":      "Sample Title 1",
				"text":       "This is the first sample document text.",
				"created_on": "2024-09-22",
			},
			{
				"title":      "Sample Title 2",
				"text":       "This is the second sample document text.",
				"created_on": "2024-09-23",
			},
			{
				"title":      "Sample Title 3",
				"text":       "This is the third sample document text.",
				"created_on": "2024-09-24",
			},
		}

		// Belgeleri ekle
		for _, doc := range documents {
			_, err := es.Index("my_index").Document(doc).Do(ctx)
			if err != nil {
				log.Printf("Belge ekleme hatası: %v", err)
			}
		}
	*/

	// Tüm belgeleri say
	count, err := es.Count().Index("my_index").Do(ctx)
	if err != nil {
		log.Fatal("Sayma hatası:", err)
	}
	fmt.Printf("İndeksteki toplam belge sayısı: %d\n", count.Count)

	count, err = es.Count().Index("my_index").Do(ctx)
	if err != nil {
		log.Fatal("Filtreli sayma hatası:", err)
	}
	fmt.Printf("24 Eylül 2024 tarihli belge sayısı: %d\n", count.Count)
}
