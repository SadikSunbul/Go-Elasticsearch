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

	// Normal object indeksi oluştur
	//CreateAuthorIndex(es, "object_index")
	//CreateAuthorDocument(es, "object_index")

	// Flattened object indeksi oluştur
	//CreateFlattenedAuthorIndex(es, "flattened_object_index")
	//CreateFlattenedAuthorDocument(es, "flattened_object_index")

	// Nested object indeksi oluştur
	CreateNestedUserIndex(es, "nested_user_index")
	CreateNestedUserDocument(es, "nested_user_index")
}

func CreateAuthorIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce varolan indeksi sil
	_, err := es.Indices.Delete(indexName).
		IgnoreUnavailable(true).
		Do(context.Background())

	if err != nil {
		log.Fatalf("İndeks silme hatası: %v", err)
	}
	/*
		Object (Nesne) Tipi:
		İç içe nesneleri ayrı ayrı indeksler
		Her alt alan için ayrı mapping oluşturur
		Arama yaparken tam yol belirtmeniz gerekir (örn: author.first_name)
		Daha fazla alan indeksler ve daha fazla depolama alanı kullanır
	*/

	// Yeni indeksi oluştur
	respons, err := es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"author":     types.NewObjectProperty(),
					"sell_count": types.NewIntegerNumberProperty(),
				},
			},
		}).
		Do(context.Background())

	if err != nil {
		log.Fatalf("İndeks oluşturma hatası: %v", err)
	}
	fmt.Printf("İndeks başarıyla oluşturuldu: %v\n", respons)
}

func CreateAuthorDocument(es *elasticsearch.TypedClient, indexName string) {
	document := map[string]interface{}{
		"author": map[string]interface{}{
			"first_name": "Imad",
			"last_name":  "Saddik",
		},
		"sell_count": 100,
	}

	response, err := es.Index(indexName).
		Request(document).
		Do(context.Background())

	if err != nil {
		log.Fatalf("Döküman oluşturma hatası: %v", err)
	}

	fmt.Printf("Döküman başarıyla oluşturuldu:\n")
	fmt.Printf("Index: %s\n", response.Index_)
	fmt.Printf("ID: %s\n", response.Id_)
	fmt.Printf("Version: %d\n", response.Version_)
	fmt.Printf("Result: %s\n", response.Result)
	fmt.Printf("Shards: %+v\n", response.Shards_)
}

func CreateFlattenedAuthorIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce varolan indeksi sil
	_, err := es.Indices.Delete(indexName).
		IgnoreUnavailable(true).
		Do(context.Background())

	if err != nil {
		log.Fatalf("İndeks silme hatası: %v", err)
	}
	/*
		Flattened Object (Düzleştirilmiş Nesne) Tipi:
		Tüm alt alanları tek bir alan olarak indeksler
		Mapping'de sadece ana alanı tanımlarsınız
		Daha az depolama alanı kullanır
		Arama yaparken daha esnek olabilir
		Performans açısından daha verimli olabilir
	*/

	// Yeni indeksi oluştur
	respons, err := es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"author": types.NewFlattenedProperty(),
				},
			},
		}).
		Do(context.Background())

	if err != nil {
		log.Fatalf("İndeks oluşturma hatası: %v", err)
	}
	fmt.Printf("İndeks başarıyla oluşturuldu: %v\n", respons)
}

func CreateFlattenedAuthorDocument(es *elasticsearch.TypedClient, indexName string) {
	document := map[string]interface{}{
		"author": map[string]interface{}{
			"first_name": "Imad",
			"last_name":  "Saddik",
		},
	}

	response, err := es.Index(indexName).
		Request(document).
		Do(context.Background())

	if err != nil {
		log.Fatalf("Döküman oluşturma hatası: %v", err)
	}

	fmt.Printf("Döküman başarıyla oluşturuldu:\n")
	fmt.Printf("Index: %s\n", response.Index_)
	fmt.Printf("ID: %s\n", response.Id_)
	fmt.Printf("Version: %d\n", response.Version_)
	fmt.Printf("Result: %s\n", response.Result)
	fmt.Printf("Shards: %+v\n", response.Shards_)
}

func CreateNestedUserIndex(es *elasticsearch.TypedClient, indexName string) {
	// Önce varolan indeksi sil
	_, err := es.Indices.Delete(indexName).
		IgnoreUnavailable(true).
		Do(context.Background())

	if err != nil {
		log.Fatalf("İndeks silme hatası: %v", err)
	}

	/*
		Nested Object (İç İçe Nesne) Tipi:
		İç içe nesneleri ayrı dökümanlar olarak indeksler
		Her alt nesne için ayrı bir Lucene dökümanı oluşturur
		Alt nesneler arasındaki ilişkileri korur
		Dizi içindeki nesneleri ayrı ayrı sorgulayabilirsiniz
		Daha fazla depolama alanı kullanır
	*/

	// Yeni indeksi oluştur
	respons, err := es.Indices.Create(indexName).
		Request(&create.Request{
			Mappings: &types.TypeMapping{
				Properties: map[string]types.Property{
					"user": types.NewNestedProperty(),
				},
			},
		}).
		Do(context.Background())

	if err != nil {
		log.Fatalf("İndeks oluşturma hatası: %v", err)
	}
	fmt.Printf("İndeks başarıyla oluşturuldu: %v\n", respons)
}

func CreateNestedUserDocument(es *elasticsearch.TypedClient, indexName string) {
	// Dizi içinde nesneler
	users := []map[string]interface{}{
		{
			"first": "John",
			"last":  "Smith",
		},
		{
			"first": "Imad",
			"last":  "Saddik",
		},
	}

	// Ana döküman
	document := map[string]interface{}{
		"user": users,
	}

	response, err := es.Index(indexName).
		Request(document).
		Do(context.Background())

	if err != nil {
		log.Fatalf("Döküman oluşturma hatası: %v", err)
	}

	fmt.Printf("Döküman başarıyla oluşturuldu:\n")
	fmt.Printf("Index: %s\n", response.Index_)
	fmt.Printf("ID: %s\n", response.Id_)
	fmt.Printf("Version: %d\n", response.Version_)
	fmt.Printf("Result: %s\n", response.Result)
	fmt.Printf("Shards: %+v\n", response.Shards_)
}
