package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
)

func main() {
	// Elasticsearch istemcisini oluştur
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Elasticsearch istemcisi oluşturulamadı: %s", err)
	}

	// İndeksi sil (varsa)
	deleteIndex, err := es.Indices.Delete([]string{"my_index"})
	if err != nil {
		log.Fatalf("İndeks silinemedi: %s", err)
	}
	defer deleteIndex.Body.Close()

	// Yeni indeks oluştur
	createIndex, err := es.Indices.Create("my_index")
	if err != nil {
		log.Fatalf("İndeks oluşturulamadı: %s", err)
	}
	defer createIndex.Body.Close()

	// dummy_data.json dosyasını oku
	data, err := os.ReadFile("../09-Get-Documents/dummy_data.json")
	if err != nil {
		log.Fatalf("Dosya okunamadı: %s", err)
	}

	// JSON verilerini ayrıştır
	var documents []map[string]interface{}
	if err := json.Unmarshal(data, &documents); err != nil {
		log.Fatalf("JSON ayrıştırılamadı: %s", err)
	}

	// Belge ID'lerini saklamak için dizi
	var documentIDs []string

	// Her belgeyi indeksle
	for _, doc := range documents {
		// Belgeyi JSON formatına dönüştür
		docJSON, err := json.Marshal(doc)
		if err != nil {
			log.Printf("Belge JSON'a dönüştürülemedi: %s", err)
			continue
		}

		// Belgeyi indeksle
		res, err := es.Index(
			"my_index",
			strings.NewReader(string(docJSON)),
			es.Index.WithContext(context.Background()),
		)
		if err != nil {
			log.Printf("Belge indekslenemedi: %s", err)
			continue
		}
		defer res.Body.Close()

		// Yanıtı ayrıştır
		var result map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
			log.Printf("Yanıt ayrıştırılamadı: %s", err)
			continue
		}

		// Belge ID'sini kaydet
		if id, ok := result["_id"].(string); ok {
			documentIDs = append(documentIDs, id)
		}
	}

	fmt.Println("Belge ID'leri:", documentIDs)

	// İndeks varlığını kontrol et
	exists, err := es.Indices.Exists([]string{"my_index"})
	if err != nil {
		log.Fatalf("İndeks varlığı kontrol edilemedi: %s", err)
	}
	defer exists.Body.Close()

	fmt.Println("İndeks mevcut mu:", exists.StatusCode == 200)

	// Belge varlığını kontrol et
	if len(documentIDs) > 0 {
		docExists, err := es.Exists(
			"my_index",
			documentIDs[0],
		)
		if err != nil {
			log.Fatalf("Belge varlığı kontrol edilemedi: %s", err)
		}
		defer docExists.Body.Close()

		fmt.Println("Belge mevcut mu:", docExists.StatusCode == 200)
	}
}
