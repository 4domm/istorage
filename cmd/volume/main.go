package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	images "github.com/4domm/images"
)

func main() {
	cfg, err := images.LoadVolumeConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	store, err := images.OpenStore(cfg)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close store: %v", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	images.StartHeartbeatLoop(ctx, store, cfg)
	log.Printf("volume server %s listening on %s", cfg.ServerID, cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, images.NewVolumeHandler(store, cfg)); err != nil {
		log.Fatal(err)
	}
}
