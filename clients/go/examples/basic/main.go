package main

import (
	"fmt"
	"log"
	"time"

	celrix "github.com/YASSERRMD/celrix/clients/go"
)

func main() {
	var client *celrix.Client
	var err error

	// Retry connection
	for i := 0; i < 5; i++ {
		client, err = celrix.Connect("127.0.0.1:6380")
		if err == nil {
			break
		}
		fmt.Printf("Connection attempt %d failed: %v\n", i, err)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Close()

	fmt.Println("Ping...")
	if err := client.Ping(); err != nil {
		log.Fatal("Ping failed:", err)
	}
	fmt.Println("Pong!")

	fmt.Println("Setting key...")
	if err := client.Set("hello_go", "world_go"); err != nil {
		log.Fatal("Set failed:", err)
	}

	fmt.Println("Getting key...")
	val, found, err := client.Get("hello_go")
	if err != nil {
		log.Fatal("Get failed:", err)
	}
	fmt.Printf("Got: %s (found: %v)\n", val, found)
	if val != "world_go" {
		log.Fatal("Value mismatch")
	}

	fmt.Println("Testing Vector operations...")
	vector := make([]float32, 1536)
	for i := range vector {
		vector[i] = 0.1
	}

	if err := client.VAdd("v_go", vector); err != nil {
		log.Fatal("VAdd failed:", err)
	}
	fmt.Println("VAdd success")

	results, err := client.VSearch(vector, 5)
	if err != nil {
		log.Fatal("VSearch failed:", err)
	}
	fmt.Printf("VSearch results: %v\n", results)

	foundKey := false
	for _, res := range results {
		if res == "v_go" {
			foundKey = true
			break
		}
	}
	if !foundKey {
		log.Fatal("v_go not found in results")
	}

	fmt.Println("All Go tests passed!")
}
