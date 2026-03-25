package chunk

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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

func (c *Client) chunkURL(baseURL, chunkID string) string {
	return strings.TrimRight(baseURL, "/") + "/chunks/" + chunkID
}

func (c *Client) PutChunk(ctx context.Context, baseURL, chunkID string, data []byte) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.chunkURL(baseURL, chunkID), bytes.NewReader(data))
	if err != nil {
		return false, err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return false, fmt.Errorf("chunk put failed: %s", res.Status)
	}
	return res.StatusCode == http.StatusCreated, nil
}

func (c *Client) GetChunk(ctx context.Context, baseURL, chunkID string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.chunkURL(baseURL, chunkID), nil)
	if err != nil {
		return nil, err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("chunk not found")
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("chunk get failed: %s", res.Status)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (c *Client) DeleteChunk(ctx context.Context, baseURL, chunkID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.chunkURL(baseURL, chunkID), nil)
	if err != nil {
		return err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("chunk delete failed: %s", res.Status)
	}
	return nil
}

func (c *Client) BatchGetChunks(ctx context.Context, baseURL string, chunkIDs []string) (map[string][]byte, error) {
	payload, err := json.Marshal(map[string][]string{"chunk_ids": chunkIDs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/chunks/batch-get", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("batch chunk not found")
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("chunk batch get failed: %s", res.Status)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return decodeBatchResponse(body)
}

func decodeBatchResponse(body []byte) (map[string][]byte, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("invalid batch chunk framing: missing item count")
	}
	count := int(binary.BigEndian.Uint32(body[:4]))
	body = body[4:]
	chunks := make(map[string][]byte, count)
	for i := 0; i < count; i++ {
		if len(body) < 4 {
			return nil, fmt.Errorf("invalid batch chunk framing: missing id length")
		}
		idLen := int(binary.BigEndian.Uint32(body[:4]))
		body = body[4:]
		if len(body) < idLen+8 {
			return nil, fmt.Errorf("invalid batch chunk framing: truncated id or size")
		}
		chunkID := string(body[:idLen])
		body = body[idLen:]
		dataLen := int(binary.BigEndian.Uint64(body[:8]))
		body = body[8:]
		if len(body) < dataLen {
			return nil, fmt.Errorf("invalid batch chunk framing: truncated payload")
		}
		chunks[chunkID] = append([]byte(nil), body[:dataLen]...)
		body = body[dataLen:]
	}
	if len(body) != 0 {
		return nil, fmt.Errorf("invalid batch chunk framing: trailing bytes")
	}
	return chunks, nil
}
