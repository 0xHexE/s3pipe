package torrentdownloader

import (
	"context"
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	s3pipe_clientimport "s3pipe_client"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
)

type TorrentClient struct {
	client          *torrent.Client
	mutex           sync.Mutex
	active          map[string]*activeTorrent
	downloadPath    string
	enableProgress  bool
	wg              sync.WaitGroup
	maxParallel     int32
	currentActive   int32
	clientImpImport *s3pipe_clientimport.S3PipeClient
	torrentUIDs     map[string]int
	ExposeStream    *ConfigExposeURL `yaml:"exposeStream"`
}

type activeTorrent struct {
	torrent  *torrent.Torrent
	cancel   context.CancelFunc
	progress float64
}

type ConfigExposeURL struct {
	Type   string `yaml:"type"`
	Config ConfigExposeHTTPServer
}

type ConfigExposeHTTPServer struct {
	Url *string `yaml:"url"`
}

type TorrentClientConfig struct {
	DownloadPath   string           `yaml:"downloadPath"`
	EnableUPnP     bool             `yaml:"enableUPnP"`
	ListenPort     int              `yaml:"listenPort"`
	EnableProgress bool             `yaml:"enableProgress"`
	DatabasePath   string           `yaml:"databasePath"`
	MaxParallel    int              `yaml:"maxParallel"`
	ExposeStream   *ConfigExposeURL `yaml:"exposeStream"`
}

func NewTorrentClient(config TorrentClientConfig, clientImpImport *s3pipe_clientimport.S3PipeClient) (*TorrentClient, error) {
	clientConfig := torrent.NewDefaultClientConfig()
	clientConfig.DataDir = config.DownloadPath
	clientConfig.NoUpload = false
	clientConfig.Seed = true
	clientConfig.NoDHT = false
	clientConfig.DisableUTP = false
	clientConfig.ListenPort = config.ListenPort
	clientConfig.DefaultStorage = storage.NewFileByInfoHash(config.DownloadPath)

	if !config.EnableUPnP {
		clientConfig.NoDefaultPortForwarding = true
	}

	client, err := torrent.NewClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create torrent client: %w", err)
	}

	td := &TorrentClient{
		client:          client,
		active:          make(map[string]*activeTorrent),
		downloadPath:    config.DownloadPath,
		enableProgress:  config.EnableProgress,
		maxParallel:     int32(config.MaxParallel),
		currentActive:   0,
		clientImpImport: clientImpImport,
		ExposeStream:    config.ExposeStream,
		torrentUIDs:     make(map[string]int), // Initialize the new map
	}

	return td, nil
}

func (td *TorrentClient) Download(magnetOrURL string, uid int) error {
	for atomic.LoadInt32(&td.currentActive) >= td.maxParallel {
		time.Sleep(time.Second)
	}

	atomic.AddInt32(&td.currentActive, 1)

	t, err := td.client.AddMagnet(magnetOrURL)
	if err != nil {
		atomic.AddInt32(&td.currentActive, -1)
		return fmt.Errorf("failed to add magnet: %w", err)
	}

	td.mutex.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	torrentHash := t.InfoHash().HexString()
	td.active[torrentHash] = &activeTorrent{
		torrent:  t,
		cancel:   cancel,
		progress: 0,
	}
	td.torrentUIDs[torrentHash] = uid // Store the UID
	enableProgress := td.enableProgress
	td.mutex.Unlock()

	td.wg.Add(1)
	go func() {
		defer td.wg.Done()
		defer atomic.AddInt32(&td.currentActive, -1)

		<-t.GotInfo()

		t.DownloadAll()

		progressTicker := time.NewTicker(time.Second)
		defer progressTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-progressTicker.C:
				progress := float64(t.BytesCompleted()) * 100 / float64(t.Length())
				td.mutex.Lock()
				td.active[torrentHash].progress = progress
				currentUID := td.torrentUIDs[torrentHash]
				td.mutex.Unlock()

				status := "downloading"
				if t.Complete().Bool() {
					status = "completed"
				}

				var streamURL string
				if td.ExposeStream != nil {
					streamURL = fmt.Sprintf("%s", strings.TrimSuffix(*td.ExposeStream.Config.Url, "/"))
				}

				files := make([]s3pipe_clientimport.TorrentFile, 0, len(t.Files()))
				for _, f := range t.Files() {
					files = append(files, s3pipe_clientimport.TorrentFile{
						Path:           f.Path(),
						Sha1:           f.FileInfo().Sha1,
						BytesCompleted: f.BytesCompleted(),
						Priority:       int(f.Priority()),
						TotalSize:      f.Length(),
					})
				}

				err := td.clientImpImport.UpdateTorrentProgress(currentUID, progress, status, files, &streamURL)
				if err != nil {
					fmt.Printf("Failed to report status for torrent id %d: %v\n", currentUID, err)
				}

				if enableProgress {
					fmt.Printf("\rDownload progress: %.2f%%", progress)
				}

				if t.Complete().Bool() {
					if enableProgress {
						fmt.Printf("\nDownload completed for %s\n", t.Name())
					}
					td.mutex.Lock()
					delete(td.active, t.InfoHash().HexString())
					td.mutex.Unlock()
					return
				}
			}
		}
	}()

	return nil
}

func (td *TorrentClient) WaitForDownloads() {
	td.wg.Wait()
}

func (td *TorrentClient) GetInfo(magnetOrURL string) (error, *metainfo.Info) {
	t, err := td.client.AddMagnet(magnetOrURL)

	if err != nil {
		return err, nil
	}

	<-t.GotInfo()

	return nil, t.Info()
}

func (td *TorrentClient) Close() []error {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	var errors []error

	for hash, active := range td.active {
		active.cancel()

		progress := float64(active.torrent.BytesCompleted()) * 100 / float64(active.torrent.Length())

		uid, exists := td.torrentUIDs[hash]
		if !exists {
			fmt.Printf("Warning: UID not found for torrent %s\n", hash)
			uid = 0
		}

		files := make([]s3pipe_clientimport.TorrentFile, 0, len(active.torrent.Files()))
		for _, f := range active.torrent.Files() {
			files = append(files, s3pipe_clientimport.TorrentFile{
				Path:           f.Path(),
				Sha1:           f.FileInfo().Sha1,
				BytesCompleted: f.BytesCompleted(),
				Priority:       int(f.Priority()),
				TotalSize:      f.Length(),
			})
		}

		err := td.clientImpImport.UpdateTorrentProgress(uid, progress, "exited", files, nil)
		if err != nil {
			fmt.Printf("Failed to update status for torrent %s: %v\n", hash, err)
			errors = append(errors, err)
		}

		if active.torrent.Info() != nil {
			active.torrent.Drop()
		}
	}

	// Clear the maps
	td.active = make(map[string]*activeTorrent)
	td.torrentUIDs = make(map[string]int)

	td.wg.Wait()
	if err := td.client.Close(); err != nil {
		return err
	}

	return errors
}
