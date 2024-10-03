package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"os/signal"
	s3pipe_clientimport "s3pipe_client"
	"sync"
	"syscall"
	"time"
	"torrent_downloader"
)

type Config struct {
	torrentdownloader.TorrentClientConfig `yaml:",inline"`
	CheckInterval                         int    `yaml:"checkInterval"`
	S3PipeBaseURL                         string `yaml:"s3pipeBaseURL"`
	S3PipeToken                           string `yaml:"s3pipeToken"`
}

func main() {
	log.Println("Starting torrent downloader application...")

	config, err := loadConfig("downloader_config.yaml")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	s3pipeClient := s3pipe_clientimport.NewS3PipeClient(config.S3PipeBaseURL, config.S3PipeToken)

	td, err := torrentdownloader.NewTorrentClient(config.TorrentClientConfig, s3pipeClient)
	if err != nil {
		log.Fatalf("Error creating torrent downloader: %v", err)
	}

	defer func() {
		log.Println("Closing torrent downloader...")
		err := td.Close()

		if len(err) == 0 {
			fmt.Printf("Closed torrent succesfully")
			return
		}
		fmt.Printf("Failed %v", err)
	}()

	if err := os.MkdirAll(config.DownloadPath, 0755); err != nil {
		log.Fatalf("Error creating output directory: %v", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup
	shutdown := make(chan struct{})

	wg.Add(1)
	go worker(td, s3pipeClient, config.CheckInterval, &wg, shutdown)

	<-c
	log.Println("Received interrupt signal. Initiating graceful shutdown...")

	close(shutdown)
	wg.Wait()

	log.Println("All workers have finished. Exiting.")
}

func worker(td *torrentdownloader.TorrentClient, s3pipeClient *s3pipe_clientimport.S3PipeClient, checkInterval int, wg *sync.WaitGroup, shutdown <-chan struct{}) {
	defer wg.Done()
	log.Println("Worker started")

	ticker := time.NewTicker(time.Duration(checkInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("Checking for new magnet links...")
			newMagnets, err := checkForNewMagnet(s3pipeClient)
			if err != nil {
				log.Printf("Error checking for new magnet links: %v", err)
				continue
			}

			for _, magnet := range newMagnets {
				if err := td.Download(magnet.URL, magnet.ID); err != nil {
					log.Printf("Error starting download for %v: %v", magnet, err)
				}
			}

		case <-shutdown:
			log.Println("Worker received shutdown signal. Exiting.")
			return
		}
	}
}

func checkForNewMagnet(client *s3pipe_clientimport.S3PipeClient) ([]s3pipe_clientimport.Torrent, error) {
	torrents, err := client.Poll()
	if err != nil {
		return nil, fmt.Errorf("error polling for new torrents: %w", err)
	}

	if len(torrents) != 0 {
		log.Printf("quequed %d torrents", len(torrents))
	}

	var newMagnets []s3pipe_clientimport.Torrent
	for _, torrent := range torrents {
		newMagnets = append(newMagnets, torrent)
		err := client.Acknowledge(torrent.ID)
		if err != nil {
			log.Printf("Error acknowledging torrent %d: %v", torrent.ID, err)
		}
	}

	return newMagnets, nil
}

func loadConfig(filename string) (*Config, error) {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	config := &Config{}
	err = yaml.Unmarshal(buf, config)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return config, nil
}
