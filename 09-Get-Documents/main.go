package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
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

	// JSON dosyasını oku
	data, err := ioutil.ReadFile("dummy_data.json")
	if err != nil {
		log.Fatal("JSON dosyası okuma hatası:", err)
	}

	// JSON verilerini parse et
	var documents []map[string]interface{}
	if err := json.Unmarshal(data, &documents); err != nil {
		log.Fatal("JSON parse hatası:", err)
	}

	// Her belgeyi Elasticsearch'e ekle
	var documentIDs []string
	for _, doc := range documents {
		resp, err := es.Index("my_index").Document(doc).Do(ctx)
		if err != nil {
			log.Printf("Belge ekleme hatası: %v", err)
			continue
		}
		documentIDs = append(documentIDs, resp.Id_)
	}

	fmt.Printf("Eklenen belge ID'leri: %v\n", documentIDs)

	// İlk belgeyi getir
	if len(documentIDs) > 0 {
		resp, err := es.Get("my_index", documentIDs[0]).Do(ctx)
		if err != nil {
			log.Printf("Belge getirme hatası: %s", err)
		} else {
			fmt.Printf("İlk belge: %+s\n", resp)
		}
	}
}
