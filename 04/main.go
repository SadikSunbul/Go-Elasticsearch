package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// Ürün yapısı
type Product struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Brand       string    `json:"brand"`
	Category    string    `json:"category"`
	Price       float64   `json:"price"`
	Color       string    `json:"color"`
	Size        string    `json:"size"`
	Rating      float64   `json:"rating"`
	StockCount  int       `json:"stock_count"`
	SoldCount   int       `json:"sold_count"`
	CreateDate  time.Time `json:"create_date"`
	IsAvailable bool      `json:"is_available"`
}

// Elasticsearch bağlantısını oluşturan fonksiyon
func createESClient() (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Örnek ürün verilerini ekleyen fonksiyon
func addSampleProducts(client *elasticsearch.Client) error {
	ctx := context.Background()
	products := []Product{
		{
			ID:          "1",
			Name:        "Nike Air Max",
			Brand:       "Nike",
			Category:    "Ayakkabı",
			Price:       1299.99,
			Color:       "Siyah",
			Size:        "42",
			Rating:      4.5,
			StockCount:  50,
			SoldCount:   150,
			CreateDate:  time.Now(),
			IsAvailable: true,
		},
	}

	for _, product := range products {
		body, err := json.Marshal(product)
		if err != nil {
			return err
		}

		_, err = client.Index(
			"products",
			bytes.NewReader(body),
			client.Index.WithContext(ctx),
			client.Index.WithDocumentID(product.ID),
		)

		if err != nil {
			return err
		}
	}
	return nil
}

// Fiyat aralığına ve kategoriye göre arama yapan fonksiyon
func searchByPriceAndCategory(client *elasticsearch.Client, minPrice, maxPrice float64, category string) ([]Product, error) {
	ctx := context.Background()

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"range": map[string]interface{}{
							"price": map[string]interface{}{
								"gte": minPrice,
								"lte": maxPrice,
							},
						},
					},
					{
						"match": map[string]interface{}{
							"category": category,
						},
					},
				},
			},
		},
		"sort": []map[string]interface{}{
			{"price": "asc"},
		},
	}

	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex("products"),
		client.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
	var products []Product
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		var product Product
		jsonData, _ := json.Marshal(source)
		if err := json.Unmarshal(jsonData, &product); err != nil {
			return nil, err
		}
		products = append(products, product)
	}
	return products, nil
}

// Çok kriterli gelişmiş arama fonksiyonu
func advancedSearch(client *elasticsearch.Client, params map[string]interface{}) ([]Product, error) {
	ctx := context.Background()

	must := []map[string]interface{}{}

	if brand, ok := params["brand"].(string); ok {
		must = append(must, map[string]interface{}{
			"match": map[string]interface{}{
				"brand": brand,
			},
		})
	}

	if minRating, ok := params["min_rating"].(float64); ok {
		must = append(must, map[string]interface{}{
			"range": map[string]interface{}{
				"rating": map[string]interface{}{
					"gte": minRating,
				},
			},
		})
	}

	if inStock, ok := params["in_stock"].(bool); ok && inStock {
		must = append(must, map[string]interface{}{
			"range": map[string]interface{}{
				"stock_count": map[string]interface{}{
					"gt": 0,
				},
			},
		})
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": must,
			},
		},
		"sort": []map[string]interface{}{
			{"rating": "desc"},
		},
		"size": 20,
	}

	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex("products"),
		client.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
	var products []Product
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		var product Product
		jsonData, _ := json.Marshal(source)
		if err := json.Unmarshal(jsonData, &product); err != nil {
			return nil, err
		}
		products = append(products, product)
	}
	return products, nil
}

// En çok satanları getiren fonksiyon
func getMostSoldProducts(client *elasticsearch.Client, limit int) ([]Product, error) {
	ctx := context.Background()

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"sort": []map[string]interface{}{
			{"sold_count": "desc"},
		},
		"size": limit,
	}

	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex("products"),
		client.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
	var products []Product
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		var product Product
		jsonData, _ := json.Marshal(source)
		if err := json.Unmarshal(jsonData, &product); err != nil {
			return nil, err
		}
		products = append(products, product)
	}
	return products, nil
}

func main() {
	// Elasticsearch client oluştur
	client, err := createESClient()
	if err != nil {
		log.Fatal(err)
	}

	// Örnek ürünleri ekle
	err = addSampleProducts(client)
	if err != nil {
		log.Fatal(err)
	}

	// Fiyat ve kategoriye göre arama örneği
	products, err := searchByPriceAndCategory(client, 1000, 2000, "Ayakkabı")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Bulunan ürünler: %+v\n", products)

	// Gelişmiş arama örneği
	searchParams := map[string]interface{}{
		"brand":      "Nike",
		"min_rating": 4.0,
		"in_stock":   true,
	}
	advancedResults, err := advancedSearch(client, searchParams)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Gelişmiş arama sonuçları: %+v\n", advancedResults)

	// En çok satanları getir
	mostSold, err := getMostSoldProducts(client, 10)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("En çok satanlar: %+v\n", mostSold)
}
