package main

import (
	"log"
	"net/http"

	images "github.com/4domm/images"
)

func main() {
	cfg, err := images.LoadCoordinatorConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	registry, err := images.LoadRegistry(cfg)
	if err != nil {
		log.Fatalf("load registry: %v", err)
	}
	defer func() {
		if err := registry.Close(); err != nil {
			log.Printf("close registry: %v", err)
		}
	}()
	log.Printf("coordinator listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, images.NewCoordinatorHandler(cfg, registry)); err != nil {
		log.Fatal(err)
	}
}
