package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"istorage/api/internal/model"
)

type Client struct {
	baseURL string
	client  *http.Client
}

type placementRequest struct {
	ChunkIDs []string `json:"chunk_ids"`
}

type placement struct {
	ChunkID string `json:"chunk_id"`
	NodeID  string `json:"node_id"`
	Zone    string `json:"zone"`
	BaseURL string `json:"base_url"`
}

type placementResponse struct {
	Placements []placement `json:"placements"`
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) objectURL(bucket, key string) string {
	parts := strings.Split(key, "/")
	escaped := make([]string, 0, len(parts))
	for _, part := range parts {
		escaped = append(escaped, url.PathEscape(part))
	}
	return fmt.Sprintf("%s/objects/%s/%s", c.baseURL, url.PathEscape(bucket), strings.Join(escaped, "/"))
}

func (c *Client) PutObject(ctx context.Context, bucket, key string, body model.PutObjectRequest) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(bucket, key), bytes.NewReader(payload))
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
		return fmt.Errorf("metadata put object failed: %s", res.Status)
	}
	return nil
}

func (c *Client) GetObject(ctx context.Context, bucket, key string) (*model.ObjectMeta, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(bucket, key), nil)
	if err != nil {
		return nil, err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("metadata get object failed: %s", res.Status)
	}
	var meta model.ObjectMeta
	if err := json.NewDecoder(res.Body).Decode(&meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *Client) DeleteObject(ctx context.Context, bucket, key string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.objectURL(bucket, key), nil)
	if err != nil {
		return false, err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if res.StatusCode >= 300 {
		return false, fmt.Errorf("metadata delete object failed: %s", res.Status)
	}
	return true, nil
}

func (c *Client) ListBucketKeys(ctx context.Context, bucket string, limit *int, cursor *string) (model.ListBucketResponse, error) {
	u, err := url.Parse(fmt.Sprintf("%s/buckets/%s/objects", c.baseURL, url.PathEscape(bucket)))
	if err != nil {
		return model.ListBucketResponse{}, err
	}
	query := u.Query()
	if limit != nil {
		query.Set("limit", strconv.Itoa(*limit))
	}
	if cursor != nil {
		query.Set("cursor", *cursor)
	}
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return model.ListBucketResponse{}, err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return model.ListBucketResponse{}, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return model.ListBucketResponse{}, fmt.Errorf("metadata list bucket failed: %s", res.Status)
	}
	var response model.ListBucketResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return model.ListBucketResponse{}, err
	}
	return response, nil
}

func (c *Client) ListChunkerNodes(ctx context.Context, healthyOnly bool) ([]model.ChunkerNode, error) {
	u, err := url.Parse(c.baseURL + "/chunkers")
	if err != nil {
		return nil, err
	}
	if healthyOnly {
		q := u.Query()
		q.Set("healthy_only", "true")
		u.RawQuery = q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("metadata list chunkers failed: %s", res.Status)
	}
	var nodes []model.ChunkerNode
	if err := json.NewDecoder(res.Body).Decode(&nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (c *Client) PlaceChunks(ctx context.Context, chunkIDs []string) (map[string]model.ChunkerNode, error) {
	payload, err := json.Marshal(placementRequest{ChunkIDs: chunkIDs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/placement", bytes.NewReader(payload))
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
		return nil, fmt.Errorf("metadata placement failed: %s", res.Status)
	}
	var response placementResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, err
	}
	out := make(map[string]model.ChunkerNode, len(response.Placements))
	for _, item := range response.Placements {
		out[item.ChunkID] = model.ChunkerNode{
			NodeID:  item.NodeID,
			Zone:    item.Zone,
			BaseURL: strings.TrimRight(item.BaseURL, "/"),
			Healthy: true,
		}
	}
	return out, nil
}
