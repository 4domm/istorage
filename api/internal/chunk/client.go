package chunk

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type Client struct {
	client *http.Client
}

type batchExistsResponse struct {
	Missing []string `json:"missing"`
}

func New() *Client {
	return &Client{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) chunkURL(baseURL, chunkID string) string {
	return strings.TrimRight(baseURL, "/") + "/chunks/" + chunkID
}

func (c *Client) BatchPutChunks(ctx context.Context, baseURL string, chunks map[string][]byte) error {
	if len(chunks) == 0 {
		return nil
	}
	keys := make([]string, 0, len(chunks))
	for chunkID := range chunks {
		keys = append(keys, chunkID)
	}
	sort.Strings(keys)

	var payload bytes.Buffer
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(keys)))
	payload.Write(countBuf[:])

	for _, chunkID := range keys {
		data := chunks[chunkID]
		var idLen [4]byte
		binary.BigEndian.PutUint32(idLen[:], uint32(len(chunkID)))
		payload.Write(idLen[:])
		payload.WriteString(chunkID)
		var dataLen [8]byte
		binary.BigEndian.PutUint64(dataLen[:], uint64(len(data)))
		payload.Write(dataLen[:])
		payload.Write(data)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/chunks/batch-put", bytes.NewReader(payload.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("chunk batch put failed: %s", res.Status)
	}
	return nil
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

func (c *Client) BatchGetChunks(ctx context.Context, baseURL string, chunkIDs []string) (map[string][]byte, error) {
	if len(chunkIDs) == 0 {
		return map[string][]byte{}, nil
	}
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
	return decodeBatchGetResponse(body)
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

func (c *Client) BatchExistsChunks(ctx context.Context, baseURL string, chunkIDs []string) (map[string]bool, error) {
	payload, err := json.Marshal(map[string][]string{"chunk_ids": chunkIDs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/chunks/batch-exists", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("chunk batch exists failed: %s", res.Status)
	}
	var response batchExistsResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, err
	}
	missing := make(map[string]struct{}, len(response.Missing))
	for _, chunkID := range response.Missing {
		missing[chunkID] = struct{}{}
	}
	out := make(map[string]bool, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		_, absent := missing[chunkID]
		out[chunkID] = !absent
	}
	return out, nil
}

func decodeBatchGetResponse(body []byte) (map[string][]byte, error) {
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
