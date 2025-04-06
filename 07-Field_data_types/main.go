package main

import (
	"context"
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

	//CreateTextIndex(es, "text_index")
	//CreateTextDocument(es, "text_index")

	// Coğrafi veri tipleri için indeksler oluştur
	CreateGeoPointIndex(es, "geo_point_index")
	CreateGeoPointDocument(es, "geo_point_index")

	CreateGeoShapeIndex(es, "geo_shape_index")
	CreateGeoShapeDocuments(es, "geo_shape_index")

	CreatePointIndex(es, "point_index")
	CreatePointDocument(es, "point_index")

	// Completion indeksi oluştur ve dökümanları ekle
	//CreateCompletionIndex(es, "text_completion_index")
	//CreateCompletionDocuments(es, "text_completion_index")

}

func CreateTextIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce indeksi sil (eğer varsa)
	_, err := es.Indices.Delete(indexName).Do(context.Background())
	if err != nil {
		log.Printf("İndeks silme hatası (önemli değil): %v", err)
	}

	// Yeni indeksi oluştur
	_, err = es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					// NewTextProperty: Elasticsearch'te tam metin araması yapılabilen bir alan oluşturur
					// Bu alan tipi, metni kelimelere ayırır ve her kelimeyi ayrı ayrı indeksler
					// Böylece "merhaba dünya" gibi bir metinde "merhaba" veya "dünya" kelimelerini ayrı ayrı arayabilirsiniz
					"email_body": types.NewTextProperty(),
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		log.Fatal("indeks oluşturma hatası:", err)
	}
	fmt.Println("indeks başarıyla oluşturuldu")
}

func CreateTextDocument(es *elasticsearch.TypedClient, indexName string) {
	document := map[string]interface{}{
		"email_body": "Merhaba, bu bir test emailidir.",
	}
	_, err := es.Index(indexName).
		Request(document).
		Do(context.Background())
	if err != nil {
		log.Fatal("döküman oluşturma hatası:", err)
	}
	fmt.Println("döküman başarıyla oluşturuldu")
}

func CreateCompletionIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce indeksi sil (eğer varsa)
	_, err := es.Indices.Delete(indexName).Do(context.Background())
	if err != nil {
		log.Printf("İndeks silme hatası (önemli değil): %v", err)
	}

	// Yeni indeksi oluştur
	_, err = es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					// NewCompletionProperty: Elasticsearch'te otomatik tamamlama özelliği için kullanılır
					// Bu alan tipi, kullanıcı yazarken öneriler sunmak için optimize edilmiştir
					// Örneğin: "Ma" yazıldığında "Mars" ve "Planet" önerilerini gösterebilir
					"suggest": types.NewCompletionProperty(),
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		log.Fatal("completion indeksi oluşturma hatası:", err)
	}
	fmt.Println("completion indeksi başarıyla oluşturuldu")
}

func CreateCompletionDocuments(es *elasticsearch.TypedClient, indexName string) {
	// İlk döküman
	document1 := map[string]interface{}{
		"suggest": map[string]interface{}{
			"input": []string{"Mars", "Planet"},
		},
	}

	// İkinci döküman
	document2 := map[string]interface{}{
		"suggest": map[string]interface{}{
			"input": []string{"Andromeda", "Galaxy"},
		},
	}

	// Dökümanları ekle
	_, err := es.Index(indexName).Document(document1).Do(context.Background())
	if err != nil {
		log.Fatal("birinci döküman ekleme hatası:", err)
	}

	_, err = es.Index(indexName).Document(document2).Do(context.Background())
	if err != nil {
		log.Fatal("ikinci döküman ekleme hatası:", err)
	}

	fmt.Println("completion dökümanları başarıyla eklendi")
}

// GeoPoint tipi için fonksiyonlar
func CreateGeoPointIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce indeksi sil (eğer varsa)
	_, err := es.Indices.Delete(indexName).Do(context.Background())
	if err != nil {
		log.Printf("İndeks silme hatası (önemli değil): %v", err)
	}

	// Yeni indeksi oluştur
	_, err = es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					// NewGeoPointProperty: Coğrafi nokta verisi için kullanılır
					// Enlem ve boylam koordinatlarını saklamak için optimize edilmiştir
					"location": types.NewGeoPointProperty(),
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		log.Fatal("geo_point indeksi oluşturma hatası:", err)
	}
	fmt.Println("geo_point indeksi başarıyla oluşturuldu")
}

func CreateGeoPointDocument(es *elasticsearch.TypedClient, indexName string) {
	document := map[string]interface{}{
		"text": "Geopoint as an object using GeoJSON format",
		"location": map[string]interface{}{
			"type":        "Point",
			"coordinates": []float64{-71.34, 41.12},
		},
	}

	_, err := es.Index(indexName).Document(document).Do(context.Background())
	if err != nil {
		log.Fatal("geo_point dökümanı ekleme hatası:", err)
	}
	fmt.Println("geo_point dökümanı başarıyla eklendi")
}

// GeoShape tipi için fonksiyonlar
func CreateGeoShapeIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce indeksi sil (eğer varsa)
	_, err := es.Indices.Delete(indexName).Do(context.Background())
	if err != nil {
		log.Printf("İndeks silme hatası (önemli değil): %v", err)
	}

	// Yeni indeksi oluştur
	_, err = es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					// NewGeoShapeProperty: Karmaşık coğrafi şekiller için kullanılır
					// Çizgi, poligon gibi şekilleri saklamak için optimize edilmiştir
					"location": types.NewGeoShapeProperty(),
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		log.Fatal("geo_shape indeksi oluşturma hatası:", err)
	}
	fmt.Println("geo_shape indeksi başarıyla oluşturuldu")
}

func CreateGeoShapeDocuments(es *elasticsearch.TypedClient, indexName string) {
	// Çizgi dökümanı
	document1 := map[string]interface{}{
		"location": map[string]interface{}{
			"type": "LineString",
			"coordinates": [][]float64{
				{-77.03653, 38.897676},
				{-77.009051, 38.889939},
			},
		},
	}

	// Poligon dökümanı
	document2 := map[string]interface{}{
		"location": map[string]interface{}{
			"type": "Polygon",
			"coordinates": [][][]float64{
				{
					{100, 0},
					{101, 0},
					{101, 1},
					{100, 1},
					{100, 0},
				},
				{
					{100.2, 0.2},
					{100.8, 0.2},
					{100.8, 0.8},
					{100.2, 0.8},
					{100.2, 0.2},
				},
			},
		},
	}

	_, err := es.Index(indexName).Document(document1).Do(context.Background())
	if err != nil {
		log.Fatal("geo_shape çizgi dökümanı ekleme hatası:", err)
	}

	_, err = es.Index(indexName).Document(document2).Do(context.Background())
	if err != nil {
		log.Fatal("geo_shape poligon dökümanı ekleme hatası:", err)
	}
	fmt.Println("geo_shape dökümanları başarıyla eklendi")
}

// Point tipi için fonksiyonlar
func CreatePointIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce indeksi sil (eğer varsa)
	_, err := es.Indices.Delete(indexName).Do(context.Background())
	if err != nil {
		log.Printf("İndeks silme hatası (önemli değil): %v", err)
	}

	// Yeni indeksi oluştur
	_, err = es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					// NewPointProperty: Basit nokta verisi için kullanılır
					// X ve Y koordinatlarını saklamak için optimize edilmiştir
					"location": types.NewPointProperty(),
				},
			},
		}).
		Do(context.Background())
	if err != nil {
		log.Fatal("point indeksi oluşturma hatası:", err)
	}
	fmt.Println("point indeksi başarıyla oluşturuldu")
}

func CreatePointDocument(es *elasticsearch.TypedClient, indexName string) {
	document := map[string]interface{}{
		"location": map[string]interface{}{
			"type":        "Point",
			"coordinates": []float64{-71.34, 41.12},
		},
	}

	_, err := es.Index(indexName).Document(document).Do(context.Background())
	if err != nil {
		log.Fatal("point dökümanı ekleme hatası:", err)
	}
	fmt.Println("point dökümanı başarıyla eklendi")
}
