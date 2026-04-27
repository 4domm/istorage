package coordinator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/4domm/images/internal/common"
)

type StorageClient struct {
	http *http.Client
}

func NewStorageClient(cfg Config) *StorageClient {
	return &StorageClient{http: &http.Client{Timeout: cfg.HTTPTimeout}}
}

func (c *StorageClient) Write(primary common.Replica, packID uint32, req common.EntryWriteRequest, body []byte) error {
	return c.postBinary(primary.URL, fmt.Sprintf("/internal/packs/%d/write-primary", packID), req, body)
}

func (c *StorageClient) Delete(primary common.Replica, packID uint32, req common.EntryDeleteRequest) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequest(http.MethodPost, strings.TrimRight(primary.URL, "/")+fmt.Sprintf("/internal/packs/%d/delete-primary", packID), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("delete failed: %s", resp.Status)
	}
	return nil
}

func (c *StorageClient) Read(replica common.Replica, packID uint32, entryID uint64, guard uint32, hdr http.Header) (*http.Response, error) {
	u := strings.TrimRight(replica.URL, "/") + fmt.Sprintf("/internal/packs/%d/entries/%d?guard=%d", packID, entryID, guard)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	common.CopyHeaders(req.Header, hdr, "Range", "If-None-Match", "If-Modified-Since")
	return c.http.Do(req)
}

func (c *StorageClient) postBinary(baseURL, path string, req common.EntryWriteRequest, body []byte) error {
	meta, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("X-Entry-Meta", string(meta))
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("write failed: %s %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return nil
}
