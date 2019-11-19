package wsworker

import (
	"fmt"
	"testing"
	"time"
)

// docker rm -f testredis; docker run --name testredis  -p 6379:6379 -d redis:5-alpine redis-server --appendonly yes

func TestCacheGet(t *testing.T) {
	now := fmt.Sprintf("%d", time.Now().UnixNano())
	cache, err := NewCache([]string{"dev.subiz.net:6379"}, "", now, 2)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := cache.Get("thanh1"); err != nil {
		t.Fatal(err)
	}

	if _, _, err := cache.Get("thanh2"); err != nil {
		t.Fatal(err)
	}

	if _, _, err := cache.Get("thanh2"); err != nil {
		t.Fatal(err)
	}

	if len(cache.m) != 2 {
		t.Fatalf("should be 2 got %d", len(cache.m))
	}

	if _, _, err := cache.Get("thanh3"); err != nil {
		t.Fatal(err)
	}

	_, has1 := cache.m["thanh1"]
	_, has2 := cache.m["thanh2"]
	_, has3 := cache.m["thanh3"]
	if has1 || !has2 || !has3 {
		t.Fatalf("wrong %v, %v, %v", has1, has2, has3)
	}
	if len(cache.m) != 2 {
		t.Fatalf("should be 2 got %d", len(cache.m))
	}
}

func TestCacheSet(t *testing.T) {
	now := fmt.Sprintf("%d", time.Now().UnixNano())
	cache, err := NewCache([]string{"dev.subiz.net:6379"}, "", now, 2)
	if err != nil {
		t.Fatal(err)
	}

	if err := cache.Set("set1", []byte("meomeo1")); err != nil {
		t.Fatal(err)
	}

	if err := cache.Set("set2", []byte("meomeo2")); err != nil {
		t.Fatal(err)
	}

	if err := cache.Set("set1", []byte("meomeo11")); err != nil {
		t.Fatal(err)
	}

	if err := cache.Set("set3", []byte("meomeo3")); err != nil {
		t.Fatal(err)
	}

	if len(cache.m) != 2 {
		t.Fatalf("should be 2 got %d", len(cache.m))
	}
	_, has1 := cache.m["set1"]
	_, has2 := cache.m["set2"]
	_, has3 := cache.m["set3"]
	if has1 || !has2 || !has3 {
		t.Fatalf("wrong %v, %v, %v", has1, has2, has3)
	}

	_, has1, err = cache.Get("set1")
	if err != nil {
		t.Fatal(err)
	}
	if !has1 {
		t.Fatalf("expect true, got false")
	}

	if len(cache.m) != 2 {
		t.Fatalf("should be 2 got %d", len(cache.m))
	}
	_, has1 = cache.m["set1"]
	_, has2 = cache.m["set2"]
	_, has3 = cache.m["set3"]
	if !has1 || has2 || !has3 {
		t.Fatalf("wrong %v, %v, %v", has1, has2, has3)
	}
}
