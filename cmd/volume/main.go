package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/4domm/images/internal/volume"
)

func main() {
	cfg, err := volume.LoadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	store, err := volume.OpenStore(cfg)
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
	volume.StartHeartbeatLoop(ctx, store, cfg)
	log.Printf("volume server %s listening on %s", cfg.ServerID, cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, volume.NewHandler(store, cfg)); err != nil {
		log.Fatal(err)
	}
}
