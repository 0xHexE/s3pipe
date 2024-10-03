package s3pipe_clientimport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type S3PipeClient struct {
	httpClient *http.Client
	baseURL    string
	token      string
}

func NewS3PipeClient(baseURL, token string) *S3PipeClient {
	return &S3PipeClient{
		httpClient: &http.Client{},
		baseURL:    baseURL,
		token:      token,
	}
}

func (c *S3PipeClient) Poll() ([]Torrent, error) {
	endpoint := fmt.Sprintf("%s/poll", c.baseURL)

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("authorization", c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var torrents []Torrent
	err = json.NewDecoder(resp.Body).Decode(&torrents)
	if err != nil {
		return nil, err
	}

	return torrents, nil
}

func (c *S3PipeClient) Acknowledge(torrentID int) error {
	endpoint := fmt.Sprintf("%s/acknowledged", c.baseURL)

	payload := struct {
		Token     string `json:"token"`
		TorrentID int    `json:"torrent_id"`
	}{
		Token:     c.token,
		TorrentID: torrentID,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *S3PipeClient) UpdateTorrentProgress(torrentID int, percentageCompleted float64, status string, files []TorrentFile, streamURL *string) error {
	endpoint := fmt.Sprintf("%s/update-progress", c.baseURL)

	payload := struct {
		Token               string        `json:"token"`
		TorrentID           int           `json:"torrent_id"`
		PercentageCompleted float64       `json:"percentage_completed"`
		Status              string        `json:"status"`
		Files               []TorrentFile `json:"files"`
		StreamURL           *string       `json:"streamURL"`
	}{
		Token:               c.token,
		TorrentID:           torrentID,
		PercentageCompleted: percentageCompleted,
		Status:              status,
		Files:               files,
		StreamURL:           streamURL,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

type Torrent struct {
	ID                  int     `json:"ID"`
	URL                 string  `json:"URL"`
	Name                string  `json:"Name"`
	Status              string  `json:"Status"`
	PercentageCompleted float64 `json:"PercentageCompleted"`
}

type TorrentFile struct {
	Path           string `json:"path"`
	Sha1           string `json:"sha1"`
	BytesCompleted int64  `json:"bytes_completed"`
	Priority       int    `json:"priority"`
	TotalSize      int64  `json:"totalSize"`
}
