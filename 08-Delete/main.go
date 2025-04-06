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
	es, err := ConnectToElasticsearch()
	if err != nil {
		log.Fatal("connect to eleasticsearch is err:", err)
	}

	fmt.Println("connect info:", es.Info())

	fmt.Println("*......Connec to Elasticsearch is success......*")

	// Döküman işlemleri
	CreateIndex(es, "my_index")
	documentIDs := AddDocuments(es, "my_index")
	DeleteDocument(es, "my_index", documentIDs[0])
	DeleteNonExistentDocument(es, "my_index", "id")
}

// Döküman işlemleri için fonksiyonlar
func CreateIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce indeksi sil (eğer varsa)
	_, err := es.Indices.Delete(indexName).Do(context.Background())
	if err != nil {
		log.Printf("İndeks silme hatası (önemli değil): %v", err)
	}

	// Yeni indeksi oluştur
	_, err = es.Indices.Create(indexName).Do(context.Background())
	if err != nil {
		log.Fatal("indeks oluşturma hatası:", err)
	}
	fmt.Println("indeks başarıyla oluşturuldu")
}

func AddDocuments(es *elasticsearch.TypedClient, indexName string) []string {
	// Örnek dökümanlar
	documents := []map[string]interface{}{
		{
			"title":   "Elasticsearch Temelleri",
			"content": "Elasticsearch, açık kaynaklı bir arama motorudur.",
			"tags":    []string{"elasticsearch", "arama", "nosql"},
		},
		{
			"title":   "Go Programlama Dili",
			"content": "Go, Google tarafından geliştirilen bir programlama dilidir.",
			"tags":    []string{"go", "programlama", "google"},
		},
		{
			"title":   "Veri Yapıları",
			"content": "Veri yapıları, verileri organize etmek için kullanılır.",
			"tags":    []string{"veri yapıları", "algoritma", "programlama"},
		},
	}

	// Dökümanları ekle ve ID'lerini sakla
	var documentIDs []string
	for i, doc := range documents {
		// Döküman ID'si olarak indeks numarasını kullan
		docID := fmt.Sprintf("doc%d", i+1)

		_, err := es.Index(indexName).Id(docID).Document(doc).Do(context.Background())
		if err != nil {
			log.Fatal("döküman ekleme hatası:", err)
		}
		documentIDs = append(documentIDs, docID)
		fmt.Printf("Döküman eklendi, ID: %s\n", docID)
	}

	return documentIDs
}

func DeleteDocument(es *elasticsearch.TypedClient, indexName string, documentID string) {
	// Var olan bir dökümanı sil
	res, err := es.Delete(indexName, documentID).Do(context.Background())
	if err != nil {
		log.Fatal("döküman silme hatası:", err)
	}
	fmt.Printf("Döküman silindi, ID: %s, Sonuç: %s\n", documentID, res.Result)
}

func DeleteNonExistentDocument(es *elasticsearch.TypedClient, indexName string, documentID string) {
	// Var olmayan bir dökümanı silmeye çalış
	_, err := es.Delete(indexName, documentID).Do(context.Background())
	if err != nil {
		fmt.Printf("Beklenen hata: Döküman bulunamadı, ID: %s\n", documentID)
	}
}
