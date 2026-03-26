package chunk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	client *http.Client
}

func New() *Client {
	return &Client{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) BatchDeleteChunks(ctx context.Context, baseURL string, chunkIDs []string) error {
	if len(chunkIDs) == 0 {
		return nil
	}
	payload, err := json.Marshal(map[string][]string{"chunk_ids": chunkIDs})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(baseURL, "/")+"/chunks/batch-delete",
		bytes.NewReader(payload),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("chunk batch delete failed: %s", res.Status)
	}
	return nil
}
