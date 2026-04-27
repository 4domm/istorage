package main

import (
	"log"
	"net/http"

	"github.com/4domm/images/internal/coordinator"
)

func main() {
	cfg, err := coordinator.LoadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	registry, err := coordinator.LoadRegistry(cfg)
	if err != nil {
		log.Fatalf("load registry: %v", err)
	}
	defer func() {
		if err := registry.Close(); err != nil {
			log.Printf("close registry: %v", err)
		}
	}()
	log.Printf("coordinator listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, coordinator.NewHandler(cfg, registry)); err != nil {
		log.Fatal(err)
	}
}
