package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
)

// Document yapısı, Elasticsearch'te saklanacak belgeyi temsil eder
type Document struct {
	Title     string `json:"title"`
	Text      string `json:"text"`
	CreatedOn string `json:"created_on"`
}

// UpdateResponse, güncelleme işleminin sonucunu temsil eder
type UpdateResponse struct {
	Index   string `json:"_index"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Result  string `json:"result"`
}

// GetResponse, belge getirme işleminin sonucunu temsil eder
type GetResponse struct {
	Index   string   `json:"_index"`
	ID      string   `json:"_id"`
	Version int      `json:"_version"`
	Found   bool     `json:"found"`
	Source  Document `json:"_source"`
}

// CountResponse, belge sayma işleminin sonucunu temsil eder
type CountResponse struct {
	Count int `json:"count"`
}

func main() {
	// Elasticsearch istemcisini oluştur
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Elasticsearch istemcisi oluşturulamadı: %s", err)
	}

	// İndeks adı
	indexName := "my_index"

	// Örnek belge oluştur
	doc := Document{
		Title:     "İlk Belge",
		Text:      "Bu ilk örnek belge metnidir.",
		CreatedOn: "2024-09-22",
	}

	// Belgeyi JSON'a dönüştür
	docJSON, err := json.Marshal(doc)
	if err != nil {
		log.Fatalf("Belge JSON'a dönüştürülemedi: %s", err)
	}

	// Belgeyi indekse ekle
	res, err := es.Index(
		indexName,
		bytes.NewReader(docJSON),
		es.Index.WithContext(context.Background()),
	)
	if err != nil {
		log.Fatalf("Belge eklenemedi: %s", err)
	}
	defer res.Body.Close()

	// Yanıtı kontrol et
	if res.IsError() {
		log.Fatalf("Belge eklenirken hata oluştu: %s", res.String())
	}

	// Yanıtı ayrıştır
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		log.Fatalf("Yanıt ayrıştırılamadı: %s", err)
	}

	// Belge ID'sini al
	documentID := result["_id"].(string)
	fmt.Printf("Belge eklendi, ID: %s\n", documentID)

	// 1.1 Mevcut bir alanı güncelle
	updateField(documentID, indexName, es)

	fmt.Scanf("devam etmek için enter tuşuna basınız")

	// 1.2.1 Script kullanarak yeni bir alan ekle
	addNewFieldWithScript(documentID, indexName, es)

	fmt.Scanf("devam etmek için enter tuşuna basınız")

	// 1.2.2 Doc kullanarak yeni bir alan ekle
	addNewFieldWithDoc(documentID, indexName, es)

	fmt.Scanf("devam etmek için enter tuşuna basınız")

	// 1.3 Bir alanı kaldır
	removeField(documentID, indexName, es)

	fmt.Scanf("devam etmek için enter tuşuna basınız")

	// 2. Olmayan bir belgeyi ekle (upsert)
	upsertNonExistentDocument(indexName, es)

	fmt.Scanf("devam etmek için enter tuşuna basınız")

	// Belge sayısını kontrol et
	countDocuments(indexName, es)

	fmt.Scanf("devam etmek için enter tuşuna basınız")
}

// updateField, mevcut bir alanı günceller
func updateField(documentID, indexName string, es *elasticsearch.Client) {
	fmt.Println("\n1.1 Mevcut bir alanı güncelleme:")

	// Güncelleme isteği oluştur
	updateBody := map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.title = params.title",
			"params": map[string]interface{}{
				"title": "Yeni Başlık",
			},
		},
	}

	updateJSON, _ := json.Marshal(updateBody)

	// Güncelleme isteğini gönder
	res, err := es.Update(
		indexName,
		documentID,
		bytes.NewReader(updateJSON),
		es.Update.WithContext(context.Background()),
	)
	if err != nil {
		log.Fatalf("Güncelleme isteği gönderilemedi: %s", err)
	}
	defer res.Body.Close()

	// Yanıtı kontrol et
	if res.IsError() {
		log.Fatalf("Güncelleme sırasında hata oluştu: %s", res.String())
	}

	// Yanıtı ayrıştır
	var updateResp UpdateResponse
	if err := json.NewDecoder(res.Body).Decode(&updateResp); err != nil {
		log.Fatalf("Güncelleme yanıtı ayrıştırılamadı: %s", err)
	}

	fmt.Printf("Güncelleme yanıtı: %+v\n", updateResp)

	// Güncellenmiş belgeyi getir
	getDocument(documentID, indexName, es)
}

// addNewFieldWithScript, script kullanarak yeni bir alan ekler
func addNewFieldWithScript(documentID, indexName string, es *elasticsearch.Client) {
	fmt.Println("\n1.2.1 Script kullanarak yeni bir alan ekleme:")

	// Güncelleme isteği oluştur
	updateBody := map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.new_field = 'dummy_value'",
		},
	}

	updateJSON, _ := json.Marshal(updateBody)

	// Güncelleme isteğini gönder
	res, err := es.Update(
		indexName,
		documentID,
		bytes.NewReader(updateJSON),
		es.Update.WithContext(context.Background()),
	)
	if err != nil {
		log.Fatalf("Güncelleme isteği gönderilemedi: %s", err)
	}
	defer res.Body.Close()

	// Yanıtı kontrol et
	if res.IsError() {
		log.Fatalf("Güncelleme sırasında hata oluştu: %s", res.String())
	}

	// Yanıtı ayrıştır
	var updateResp UpdateResponse
	if err := json.NewDecoder(res.Body).Decode(&updateResp); err != nil {
		log.Fatalf("Güncelleme yanıtı ayrıştırılamadı: %s", err)
	}

	fmt.Printf("Güncelleme yanıtı: %+v\n", updateResp)

	// Güncellenmiş belgeyi getir
	getDocument(documentID, indexName, es)
}

// addNewFieldWithDoc, doc kullanarak yeni bir alan ekler
func addNewFieldWithDoc(documentID, indexName string, es *elasticsearch.Client) {
	fmt.Println("\n1.2.2 Doc kullanarak yeni bir alan ekleme:")

	// Güncelleme isteği oluştur
	updateBody := map[string]interface{}{
		"doc": map[string]interface{}{
			"new_value_2": "dummy_value_2",
		},
	}

	updateJSON, _ := json.Marshal(updateBody)

	// Güncelleme isteğini gönder
	res, err := es.Update(
		indexName,
		documentID,
		bytes.NewReader(updateJSON),
		es.Update.WithContext(context.Background()),
	)
	if err != nil {
		log.Fatalf("Güncelleme isteği gönderilemedi: %s", err)
	}
	defer res.Body.Close()

	// Yanıtı kontrol et
	if res.IsError() {
		log.Fatalf("Güncelleme sırasında hata oluştu: %s", res.String())
	}

	// Yanıtı ayrıştır
	var updateResp UpdateResponse
	if err := json.NewDecoder(res.Body).Decode(&updateResp); err != nil {
		log.Fatalf("Güncelleme yanıtı ayrıştırılamadı: %s", err)
	}

	fmt.Printf("Güncelleme yanıtı: %+v\n", updateResp)

	// Güncellenmiş belgeyi getir
	getDocument(documentID, indexName, es)
}

// removeField, bir alanı kaldırır
func removeField(documentID, indexName string, es *elasticsearch.Client) {
	fmt.Println("\n1.3 Bir alanı kaldırma:")

	// Güncelleme isteği oluştur
	updateBody := map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.remove('new_field')",
		},
	}

	updateJSON, _ := json.Marshal(updateBody)

	// Güncelleme isteğini gönder
	res, err := es.Update(
		indexName,
		documentID,
		bytes.NewReader(updateJSON),
		es.Update.WithContext(context.Background()),
	)
	if err != nil {
		log.Fatalf("Güncelleme isteği gönderilemedi: %s", err)
	}
	defer res.Body.Close()

	// Yanıtı kontrol et
	if res.IsError() {
		log.Fatalf("Güncelleme sırasında hata oluştu: %s", res.String())
	}

	// Yanıtı ayrıştır
	var updateResp UpdateResponse
	if err := json.NewDecoder(res.Body).Decode(&updateResp); err != nil {
		log.Fatalf("Güncelleme yanıtı ayrıştırılamadı: %s", err)
	}

	fmt.Printf("Güncelleme yanıtı: %+v\n", updateResp)

	// Güncellenmiş belgeyi getir
	getDocument(documentID, indexName, es)
}

// upsertNonExistentDocument, olmayan bir belgeyi ekler (upsert)
func upsertNonExistentDocument(indexName string, es *elasticsearch.Client) {
	fmt.Println("\n2. Olmayan bir belgeyi ekleme (upsert):")

	// Upsert isteği oluştur
	upsertBody := map[string]interface{}{
		"doc": map[string]interface{}{
			"book_id":   1234,
			"book_name": "Bir Kitap",
		},
		"doc_as_upsert": true,
	}

	upsertJSON, _ := json.Marshal(upsertBody)

	// Upsert isteğini gönder
	res, err := es.Update(
		indexName,
		"1", // Olmayan bir ID
		bytes.NewReader(upsertJSON),
		es.Update.WithContext(context.Background()),
	)
	if err != nil {
		log.Fatalf("Upsert isteği gönderilemedi: %s", err)
	}
	defer res.Body.Close()

	// Yanıtı kontrol et
	if res.IsError() {
		log.Fatalf("Upsert sırasında hata oluştu: %s", res.String())
	}

	// Yanıtı ayrıştır
	var updateResp UpdateResponse
	if err := json.NewDecoder(res.Body).Decode(&updateResp); err != nil {
		log.Fatalf("Upsert yanıtı ayrıştırılamadı: %s", err)
	}

	fmt.Printf("Upsert yanıtı: %+v\n", updateResp)

	// Eklenen belgeyi getir
	getDocument("1", indexName, es)
}

// getDocument, belgeyi getirir
func getDocument(documentID, indexName string, es *elasticsearch.Client) {
	// Belgeyi getir
	res, err := es.Get(
		indexName,
		documentID,
		es.Get.WithContext(context.Background()),
	)
	if err != nil {
		log.Fatalf("Belge getirilemedi: %s", err)
	}
	defer res.Body.Close()

	// Yanıtı kontrol et
	if res.IsError() {
		log.Fatalf("Belge getirme sırasında hata oluştu: %s", res.String())
	}

	// Yanıtı ayrıştır
	var getResp GetResponse
	if err := json.NewDecoder(res.Body).Decode(&getResp); err != nil {
		log.Fatalf("Belge getirme yanıtı ayrıştırılamadı: %s", err)
	}

	fmt.Printf("Belge: %+v\n", getResp)
}

// countDocuments, belge sayısını kontrol eder
func countDocuments(indexName string, es *elasticsearch.Client) {
	fmt.Println("\nBelge sayısını kontrol etme:")

	// Belge sayısını getir
	res, err := es.Count(
		es.Count.WithIndex(indexName),
		es.Count.WithContext(context.Background()),
	)
	if err != nil {
		log.Fatalf("Belge sayısı getirilemedi: %s", err)
	}
	defer res.Body.Close()

	// Yanıtı kontrol et
	if res.IsError() {
		log.Fatalf("Belge sayısı getirme sırasında hata oluştu: %s", res.String())
	}

	// Yanıtı ayrıştır
	var countResp CountResponse
	if err := json.NewDecoder(res.Body).Decode(&countResp); err != nil {
		log.Fatalf("Belge sayısı yanıtı ayrıştırılamadı: %s", err)
	}

	fmt.Printf("Belge sayısı: %d\n", countResp.Count)
}
