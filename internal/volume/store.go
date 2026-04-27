package volume

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/4domm/images/internal/common"
)

const (
	recordMagic    = "NDL1"
	recordHeaderSz = 72
	flagTombstone  = uint32(1)
)

type storedMetadata struct {
	ContentType string `json:"content_type"`
	Checksum    string `json:"checksum"`
}

type entry struct {
	EntryID     uint64               `json:"entry_id"`
	Guard       uint32               `json:"guard"`
	Flags       uint32               `json:"flags"`
	Offset      int64                `json:"offset"`
	MetadataLen uint32               `json:"metadata_len"`
	Size        uint64               `json:"size"`
	Metadata    common.ImageMetadata `json:"metadata"`
}

type packFile struct {
	id       uint32
	path     string
	idxPath  string
	maxBytes int64

	mu              sync.RWMutex
	file            *os.File
	size            int64
	state           common.PackState
	index           map[uint64]entry
	snapshotDirty   bool
	mutationVersion uint64
}

type Store struct {
	mu         sync.RWMutex
	cfg        Config
	packs      map[uint32]*packFile
	httpClient *http.Client
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

func OpenStore(cfg Config) (*Store, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	store := &Store{
		cfg:        cfg,
		packs:      make(map[uint32]*packFile),
		httpClient: &http.Client{Timeout: cfg.HTTPTimeout},
		stopCh:     make(chan struct{}),
	}
	if err := store.loadExisting(); err != nil {
		return nil, err
	}
	store.startSnapshotLoop()
	return store, nil
}

func (s *Store) Close() error {
	close(s.stopCh)
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, pack := range s.packs {
		if err := pack.snapshotNow(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := pack.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Store) loadExisting() error {
	matches, err := filepath.Glob(filepath.Join(s.cfg.DataDir, "*.dat"))
	if err != nil {
		return err
	}
	for _, match := range matches {
		var packID uint32
		if _, err := fmt.Sscanf(filepath.Base(match), "%08x.dat", &packID); err != nil {
			continue
		}
		if _, err := s.ensurePack(packID, s.cfg.MaxPackBytes); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensurePack(packID uint32, maxBytes int64) (*packFile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.packs[packID]; ok {
		return existing, nil
	}
	path := filepath.Join(s.cfg.DataDir, fmt.Sprintf("%08x.dat", packID))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	pack := &packFile{
		id:       packID,
		path:     path,
		idxPath:  filepath.Join(s.cfg.DataDir, fmt.Sprintf("%08x.idx", packID)),
		maxBytes: maxBytes,
		file:     file,
		size:     info.Size(),
		state:    common.PackStateWritable,
		index:    make(map[uint64]entry),
	}
	if err := pack.recover(); err != nil {
		_ = file.Close()
		return nil, err
	}
	if pack.size >= pack.maxBytes {
		pack.state = common.PackStateReadonly
	}
	s.packs[packID] = pack
	return pack, nil
}

func (p *packFile) recover() error {
	info, err := p.file.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()

	coveredOffset, err := p.loadSnapshot()
	if err != nil || coveredOffset > fileSize {
		p.index = make(map[uint64]entry)
		p.state = common.PackStateWritable
		coveredOffset = 0
	}
	offset, err := p.replayFrom(coveredOffset)
	if err != nil {
		if coveredOffset != 0 {
			p.index = make(map[uint64]entry)
			p.state = common.PackStateWritable
			offset, err = p.replayFrom(0)
		}
		if err != nil {
			return err
		}
	}
	p.size = offset
	if offset > coveredOffset || coveredOffset == 0 {
		p.snapshotDirty = true
		p.mutationVersion = 1
	}
	return nil
}

func (p *packFile) loadSnapshot() (int64, error) {
	raw, err := os.ReadFile(p.idxPath)
	if err != nil {
		return 0, err
	}
	var snapshot struct {
		CoveredOffset int64   `json:"covered_offset"`
		Size          int64   `json:"size"`
		State         string  `json:"state"`
		Index         []entry `json:"index"`
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return 0, err
	}
	p.index = make(map[uint64]entry, len(snapshot.Index))
	for _, item := range snapshot.Index {
		p.index[item.EntryID] = item
	}
	p.size = snapshot.Size
	p.state = common.PackState(snapshot.State)
	return snapshot.CoveredOffset, nil
}

func (p *packFile) replayFrom(offset int64) (int64, error) {
	if _, err := p.file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(p.file)
	currentOffset := offset
	for {
		item, nextOffset, err := readRecord(reader, currentOffset)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return currentOffset, nil
			}
			return 0, err
		}
		p.index[item.EntryID] = item
		currentOffset = nextOffset
	}
}

func readRecord(reader *bufio.Reader, offset int64) (entry, int64, error) {
	header := make([]byte, recordHeaderSz)
	if _, err := io.ReadFull(reader, header); err != nil {
		return entry{}, 0, err
	}
	if string(header[:4]) != recordMagic {
		return entry{}, 0, fmt.Errorf("invalid record magic at offset %d", offset)
	}
	entryID := binary.BigEndian.Uint64(header[4:12])
	guard := binary.BigEndian.Uint32(header[12:16])
	flags := binary.BigEndian.Uint32(header[16:20])
	metaLen := binary.BigEndian.Uint32(header[20:24])
	dataSize := binary.BigEndian.Uint64(header[24:32])
	checksumBytes := header[32:64]
	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(reader, metaBytes); err != nil {
		return entry{}, 0, err
	}
	payloadOffset := offset + recordHeaderSz + int64(metaLen)
	if _, err := io.CopyN(io.Discard, reader, int64(dataSize)); err != nil {
		return entry{}, 0, err
	}
	total := recordHeaderSz + int64(metaLen) + int64(dataSize)
	padding := paddedBytes(total) - total
	if padding > 0 {
		if _, err := io.CopyN(io.Discard, reader, padding); err != nil {
			return entry{}, 0, err
		}
	}
	var metadata storedMetadata
	if len(metaBytes) > 0 {
		if err := json.Unmarshal(metaBytes, &metadata); err != nil {
			return entry{}, 0, err
		}
	}
	item := entry{
		EntryID:     entryID,
		Guard:       guard,
		Flags:       flags,
		Offset:      payloadOffset,
		MetadataLen: metaLen,
		Size:        dataSize,
		Metadata: common.ImageMetadata{
			ContentType: metadata.ContentType,
			Checksum:    hex.EncodeToString(checksumBytes),
			Size:        dataSize,
		},
	}
	if metadata.Checksum != "" {
		item.Metadata.Checksum = metadata.Checksum
	}
	return item, offset + paddedBytes(total), nil
}

func (p *packFile) saveSnapshot(snapshot snapshotState) error {
	raw, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := p.idxPath + ".tmp"
	if err := os.WriteFile(tmpPath, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, p.idxPath)
}

type snapshotState struct {
	CoveredOffset int64   `json:"covered_offset"`
	Size          int64   `json:"size"`
	State         string  `json:"state"`
	Index         []entry `json:"index"`
}

func (s *Store) CreateVolume(packID uint32, maxBytes int64) error {
	_, err := s.ensurePack(packID, maxBytes)
	return err
}

func (s *Store) WritePrimary(packID uint32, req common.EntryWriteRequest, body []byte) error {
	pack, err := s.ensurePack(packID, s.cfg.MaxPackBytes)
	if err != nil {
		return err
	}
	if err := pack.append(req, body, false); err != nil {
		return err
	}
	for _, replica := range req.Replicas {
		if replica.ServerID == s.cfg.ServerID {
			continue
		}
		if err := s.replicate(replica.URL, packID, req, body); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Replicate(packID uint32, req common.EntryWriteRequest, body []byte) error {
	pack, err := s.ensurePack(packID, s.cfg.MaxPackBytes)
	if err != nil {
		return err
	}
	return pack.append(req, body, false)
}

func (s *Store) DeletePrimary(packID uint32, req common.EntryDeleteRequest) error {
	pack, err := s.ensurePack(packID, s.cfg.MaxPackBytes)
	if err != nil {
		return err
	}
	if err := pack.appendTombstone(req.EntryID, req.Guard); err != nil {
		return err
	}
	for _, replica := range req.Replicas {
		if replica.ServerID == s.cfg.ServerID {
			continue
		}
		if err := s.deleteReplica(replica.URL, packID, req); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DeleteReplica(packID uint32, req common.EntryDeleteRequest) error {
	pack, err := s.ensurePack(packID, s.cfg.MaxPackBytes)
	if err != nil {
		return err
	}
	return pack.appendTombstone(req.EntryID, req.Guard)
}

func (s *Store) Read(packID uint32, entryID uint64, guard uint32) (entry, io.ReadCloser, error) {
	pack, err := s.ensurePack(packID, s.cfg.MaxPackBytes)
	if err != nil {
		return entry{}, nil, err
	}
	return pack.read(entryID, guard)
}

func (s *Store) Compact(packID uint32) error {
	pack, err := s.ensurePack(packID, s.cfg.MaxPackBytes)
	if err != nil {
		return err
	}
	return pack.compact()
}

func (s *Store) RepairVolume(packID uint32, replicas []common.Replica) error {
	pack, err := s.ensurePack(packID, s.cfg.MaxPackBytes)
	if err != nil {
		return err
	}
	return pack.repairToReplicas(s, replicas)
}

func (s *Store) Heartbeat() common.HeartbeatRequest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	packs := make([]common.HeartbeatPack, 0, len(s.packs))
	for _, pack := range s.packs {
		pack.mu.RLock()
		packs = append(packs, common.HeartbeatPack{
			PackID: pack.id,
			State:  pack.state,
			Size:   pack.size,
		})
		pack.mu.RUnlock()
	}
	return common.HeartbeatRequest{
		ServerID:     s.cfg.ServerID,
		URL:          s.cfg.PublicURL,
		FreeBytes:    diskFreeGuess(s.cfg.MaxPackBytes, packs),
		MaxPackBytes: s.cfg.MaxPackBytes,
		Packs:        packs,
	}
}

func (s *Store) replicate(baseURL string, packID uint32, req common.EntryWriteRequest, body []byte) error {
	return postBinary(s.httpClient, baseURL, fmt.Sprintf("/internal/packs/%d/replicate", packID), req, body)
}

func (s *Store) deleteReplica(baseURL string, packID uint32, req common.EntryDeleteRequest) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+fmt.Sprintf("/internal/packs/%d/delete", packID), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("replica delete failed: %s", resp.Status)
	}
	return nil
}

func (p *packFile) append(req common.EntryWriteRequest, body []byte, tombstone bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == common.PackStateReadonly && !tombstone {
		return fmt.Errorf("pack is readonly")
	}
	meta := storedMetadata{
		ContentType: req.Metadata.ContentType,
		Checksum:    req.Metadata.Checksum,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if req.Metadata.Checksum == "" {
		sum := sha256.Sum256(body)
		req.Metadata.Checksum = hex.EncodeToString(sum[:])
	}
	checksumBytes, err := hex.DecodeString(req.Metadata.Checksum)
	if err != nil {
		return err
	}
	if len(checksumBytes) != sha256.Size {
		return fmt.Errorf("invalid checksum length")
	}
	flags := uint32(0)
	if tombstone {
		flags = flagTombstone
		body = nil
		metaBytes = nil
	}
	header := make([]byte, recordHeaderSz)
	copy(header[:4], []byte(recordMagic))
	binary.BigEndian.PutUint64(header[4:12], req.EntryID)
	binary.BigEndian.PutUint32(header[12:16], req.Guard)
	binary.BigEndian.PutUint32(header[16:20], flags)
	binary.BigEndian.PutUint32(header[20:24], uint32(len(metaBytes)))
	binary.BigEndian.PutUint64(header[24:32], uint64(len(body)))
	copy(header[32:64], checksumBytes)
	binary.BigEndian.PutUint64(header[64:72], uint64(time.Now().Unix()))

	offset := p.size
	payloadOffset := offset + recordHeaderSz + int64(len(metaBytes))
	record := bytes.NewBuffer(header)
	record.Write(metaBytes)
	record.Write(body)
	padding := paddedBytes(int64(record.Len())) - int64(record.Len())
	if padding > 0 {
		record.Write(make([]byte, padding))
	}
	if _, err := p.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if _, err := p.file.Write(record.Bytes()); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return err
	}
	p.size += int64(record.Len())
	p.index[req.EntryID] = entry{
		EntryID:     req.EntryID,
		Guard:       req.Guard,
		Flags:       flags,
		Offset:      payloadOffset,
		MetadataLen: uint32(len(metaBytes)),
		Size:        uint64(len(body)),
		Metadata:    req.Metadata,
	}
	if p.size >= p.maxBytes {
		p.state = common.PackStateReadonly
	}
	p.snapshotDirty = true
	p.mutationVersion++
	return nil
}

func (p *packFile) appendTombstone(entryID uint64, guard uint32) error {
	return p.append(common.EntryWriteRequest{
		EntryID:  entryID,
		Guard:    guard,
		Metadata: common.ImageMetadata{Checksum: strings.Repeat("0", sha256.Size*2)},
	}, nil, true)
}

func (p *packFile) read(entryID uint64, guard uint32) (entry, io.ReadCloser, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	item, ok := p.index[entryID]
	if !ok || item.Guard != guard || item.Flags&flagTombstone != 0 {
		return entry{}, nil, os.ErrNotExist
	}
	file, err := os.Open(p.path)
	if err != nil {
		return entry{}, nil, err
	}
	if _, err := file.Seek(item.Offset, io.SeekStart); err != nil {
		_ = file.Close()
		return entry{}, nil, err
	}
	return item, io.NopCloser(io.LimitReader(file, int64(item.Size))), nil
}

func (p *packFile) compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = common.PackStateCompacting
	tmpPath := p.path + ".compact"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer tmpFile.Close()

	newIndex := make(map[uint64]entry)
	var offset int64
	keys := make([]uint64, 0, len(p.index))
	for entryID, item := range p.index {
		if item.Flags&flagTombstone != 0 {
			continue
		}
		keys = append(keys, entryID)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	src, err := os.Open(p.path)
	if err != nil {
		return err
	}
	defer src.Close()
	for _, entryID := range keys {
		item := p.index[entryID]
		recordStart := item.Offset - recordHeaderSz - int64(item.MetadataLen)
		recordLength := paddedBytes(recordHeaderSz + int64(item.MetadataLen) + int64(item.Size))
		if _, err := src.Seek(recordStart, io.SeekStart); err != nil {
			return err
		}
		buf := make([]byte, recordLength)
		if _, err := io.ReadFull(src, buf); err != nil {
			return err
		}
		if _, err := tmpFile.Write(buf); err != nil {
			return err
		}
		item.Offset = offset + recordHeaderSz + int64(item.MetadataLen)
		newIndex[entryID] = item
		offset += recordLength
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := p.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, p.path); err != nil {
		return err
	}
	file, err := os.OpenFile(p.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	p.file = file
	p.index = newIndex
	p.size = offset
	if p.size < p.maxBytes {
		p.state = common.PackStateWritable
	} else {
		p.state = common.PackStateReadonly
	}
	p.snapshotDirty = true
	p.mutationVersion++
	return p.snapshotNow()
}

func (p *packFile) repairToReplicas(store *Store, replicas []common.Replica) error {
	p.mu.RLock()
	live := make([]entry, 0, len(p.index))
	for _, item := range p.index {
		if item.Flags&flagTombstone == 0 {
			live = append(live, item)
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].EntryID < live[j].EntryID })
	p.mu.RUnlock()

	src, err := os.Open(p.path)
	if err != nil {
		return err
	}
	defer src.Close()
	for _, item := range live {
		body := make([]byte, item.Size)
		if _, err := src.Seek(item.Offset, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.ReadFull(src, body); err != nil {
			return err
		}
		req := common.EntryWriteRequest{
			EntryID:  item.EntryID,
			Guard:    item.Guard,
			Metadata: item.Metadata,
		}
		for _, replica := range replicas {
			if replica.ServerID == store.cfg.ServerID {
				continue
			}
			if err := store.replicate(replica.URL, p.id, req, body); err != nil {
				return err
			}
		}
	}
	return nil
}

func paddedBytes(n int64) int64 {
	rem := n % 8
	if rem == 0 {
		return n
	}
	return n + (8 - rem)
}

func diskFreeGuess(maxPackBytes int64, packs []common.HeartbeatPack) int64 {
	var used int64
	for _, pack := range packs {
		used += pack.Size
	}
	guess := maxPackBytes*1024 - used
	if guess < 0 {
		return 0
	}
	return guess
}

func (s *Store) startSnapshotLoop() {
	if s.cfg.SnapshotInterval <= 0 {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.cfg.SnapshotInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				s.snapshotDirtyPacks()
			}
		}
	}()
}

func (s *Store) snapshotDirtyPacks() {
	s.mu.RLock()
	packs := make([]*packFile, 0, len(s.packs))
	for _, pack := range s.packs {
		packs = append(packs, pack)
	}
	s.mu.RUnlock()

	for _, pack := range packs {
		_ = pack.snapshotNow()
	}
}

func (p *packFile) snapshotNow() error {
	snapshot, version, dirty := p.buildSnapshot()
	if !dirty {
		return nil
	}
	if err := p.saveSnapshot(snapshot); err != nil {
		return err
	}
	p.mu.Lock()
	if p.mutationVersion == version {
		p.snapshotDirty = false
	}
	p.mu.Unlock()
	return nil
}

func (p *packFile) buildSnapshot() (snapshotState, uint64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.snapshotDirty {
		return snapshotState{}, 0, false
	}
	items := make([]entry, 0, len(p.index))
	for _, item := range p.index {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].EntryID < items[j].EntryID })
	return snapshotState{
		CoveredOffset: p.size,
		Size:          p.size,
		State:         string(p.state),
		Index:         items,
	}, p.mutationVersion, true
}
