package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/anacrolix/torrent"
	"github.com/dgrijalva/jwt-go"
	"github.com/glebarez/sqlite"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

type User struct {
	gorm.Model
	Name          string
	Email         string `gorm:"unique"`
	PasswordHash  string
	EmailVerified bool
}

type Worker struct {
	gorm.Model
	NickName          string
	RegistrationToken string
	IsPublic          bool
	OwnerID           uint
	Owner             *User
	Tasks             int
	TotalMBToDownload float64
}

type UserTorrent struct {
	gorm.Model
	UserID    uint
	TorrentID uint
}

type Torrent struct {
	gorm.Model
	URL                 string `gorm:"not null"`
	Name                string
	Status              string
	PercentageCompleted float64
	WorkerID            uint
	Worker              *Worker
	Size                float64 // Size in MB
	Hash                string
	StreamURL           *string
	Files               []TorrentFiles
}

type TorrentFiles struct {
	gorm.Model
	TorrentID      uint
	Path           string
	Sha1           string
	BytesCompleted int64
	Priority       int
	TotalSize      int64
}

var (
	db     *gorm.DB
	jwtKey []byte
	client *torrent.Client
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	// Initialize database
	initDB()

	err := initTorrentClient()
	if err != nil {
		log.Fatalf("Unable to initialize torrent client %v", err)
	}

	// Set JWT key
	jwtKey = []byte(os.Getenv("JWT_SECRET"))

	// Initialize router
	r := mux.NewRouter()

	// Define routes
	r.HandleFunc("/register", register).Methods("POST")
	r.HandleFunc("/login", login).Methods("POST")
	r.HandleFunc("/worker/{id}", authenticateMiddleware(getWorker)).Methods("GET")
	r.HandleFunc("/worker", authenticateMiddleware(addWorker)).Methods("POST")
	r.HandleFunc("/worker/{id}/token", authenticateMiddleware(createRegistrationToken)).Methods("POST")
	r.HandleFunc("/torrent", authenticateMiddleware(addTorrent)).Methods("POST")
	r.HandleFunc("/torrent", authenticateMiddleware(getAllTorrents)).Methods("GET")
	r.HandleFunc("/watch", authenticateMiddleware(watchStatus))

	r.HandleFunc("/poll", pollTorrents).Methods("GET")
	r.HandleFunc("/acknowledged", acknowledgeTorrent).Methods("POST")
	r.HandleFunc("/update-progress", updateTorrentProgress).Methods("POST")

	// Configure server
	srv := &http.Server{
		Addr:         ":" + os.Getenv("PORT"),
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      r,
	}

	// Run server in a goroutine so that it doesn't block
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Println(err)
		}
	}()

	// Graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	err = srv.Shutdown(ctx)
	if err != nil {
		log.Printf("error shutting down")
		return
	}
	log.Println("shutting down")
	os.Exit(0)
}

func initDB() {
	var err error
	dbType := os.Getenv("DB_TYPE")

	switch dbType {
	case "sqlite":
		db, err = gorm.Open(sqlite.Open(os.Getenv("SQLITE_DB_PATH")), &gorm.Config{})
	case "postgres":
		dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC",
			os.Getenv("DB_HOST"),
			os.Getenv("DB_USER"),
			os.Getenv("DB_PASSWORD"),
			os.Getenv("DB_NAME"),
			os.Getenv("DB_PORT"),
		)
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	default:
		log.Fatal("Unsupported database type. Please use 'sqlite' or 'postgres'")
	}

	if err != nil {
		log.Fatal("failed to connect database")
	}

	// Migrate the schema
	err = db.AutoMigrate(&User{}, &Worker{}, &Torrent{}, &UserTorrent{}, &TorrentFiles{})
	if err != nil {
		log.Fatal("failed to migrate database")
	}
}

func authenticateMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, err := getUserIDFromToken(r)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	}
}

func register(w http.ResponseWriter, r *http.Request) {
	var user User
	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.PasswordHash), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "Error hashing password", http.StatusInternalServerError)
		return
	}

	user.PasswordHash = string(hashedPassword)

	if err := db.Create(&user).Error; err != nil {
		http.Error(w, "Error creating user", http.StatusInternalServerError)
		return
	}

	token, err := generateToken(user.ID)
	if err != nil {
		http.Error(w, "Error generating token", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"token": token})
}

func login(w http.ResponseWriter, r *http.Request) {
	var loginData struct {
		Email    string
		Password string
	}
	if err := json.NewDecoder(r.Body).Decode(&loginData); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var user User
	if err := db.Where("email = ?", loginData.Email).First(&user).Error; err != nil {
		http.Error(w, "User not found", http.StatusUnauthorized)
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(loginData.Password)); err != nil {
		http.Error(w, "Invalid password", http.StatusUnauthorized)
		return
	}

	token, err := generateToken(user.ID)
	if err != nil {
		http.Error(w, "Error generating token", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"token": token})
}

func getWorker(w http.ResponseWriter, r *http.Request) {
	userID, _ := getUserIDFromToken(r)

	vars := mux.Vars(r)
	workerID := vars["id"]

	var worker Worker
	if err := db.First(&worker, workerID).Error; err != nil {
		http.Error(w, "Worker not found", http.StatusNotFound)
		return
	}

	if !worker.IsPublic && worker.OwnerID != userID {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(worker)
}

func addWorker(w http.ResponseWriter, r *http.Request) {
	userID, _ := getUserIDFromToken(r)

	var worker Worker
	if err := json.NewDecoder(r.Body).Decode(&worker); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	worker.OwnerID = userID

	if err := db.Create(&worker).Error; err != nil {
		http.Error(w, "Error creating worker", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(worker)
}

func createRegistrationToken(w http.ResponseWriter, r *http.Request) {
	userID, _ := getUserIDFromToken(r)

	vars := mux.Vars(r)
	workerID := vars["id"]

	var worker Worker
	if err := db.First(&worker, workerID).Error; err != nil {
		http.Error(w, "Worker not found", http.StatusNotFound)
		return
	}

	if worker.OwnerID != userID {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	token, err := generateRegistrationToken()
	if err != nil {
		http.Error(w, "Unable to generate token", http.StatusInternalServerError)
		return
	}

	worker.RegistrationToken = token
	if err := db.Save(&worker).Error; err != nil {
		http.Error(w, "Error saving worker", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(map[string]string{"registration_token": token})
	if err != nil {
		http.Error(w, "Something went wrong", http.StatusInternalServerError)
		return
	}
}

func addTorrent(w http.ResponseWriter, r *http.Request) {
	userID, _ := getUserIDFromToken(r)

	var torrentInput struct {
		URL      string `json:"url"`
		Name     string `json:"name"`
		WorkerID uint   `json:"worker_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&torrentInput); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if torrentInput.URL == "" {
		http.Error(w, "URL is required", http.StatusBadRequest)
		return
	}

	// Get the torrent hash
	t, err := client.AddMagnet(torrentInput.URL)
	if err != nil {
		http.Error(w, "Error adding magnet URL", http.StatusInternalServerError)
		return
	}
	defer t.Drop()

	<-t.GotInfo()
	hash := t.InfoHash().HexString()

	// Check if the torrent already exists
	var existingTorrent Torrent
	if err := db.Where("hash = ?", hash).First(&existingTorrent).Error; err == nil {
		// Torrent exists, add it to user_torrents
		userTorrent := UserTorrent{
			UserID:    userID,
			TorrentID: existingTorrent.ID,
		}
		if err := db.Create(&userTorrent).Error; err != nil {
			http.Error(w, "Error creating user torrent association", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(existingTorrent)
		return
	}

	// Torrent doesn't exist, create a new one
	torrentFile := Torrent{
		URL:    torrentInput.URL,
		Name:   torrentInput.Name,
		Hash:   hash,
		Status: "queued",
	}

	// Assign worker
	if torrentInput.WorkerID == 0 {
		var worker Worker
		if err := db.Where("owner_id = ?", userID).First(&worker).Error; err != nil {
			if err := db.Where("is_public = ?", true).First(&worker).Error; err != nil {
				http.Error(w, "No available worker found", http.StatusBadRequest)
				return
			}
		}
		torrentFile.WorkerID = worker.ID
	} else {
		var worker Worker
		if err := db.First(&worker, torrentInput.WorkerID).Error; err != nil {
			http.Error(w, "Invalid worker", http.StatusBadRequest)
			return
		}
		if !worker.IsPublic && worker.OwnerID != userID {
			http.Error(w, "Unauthorized to use this worker", http.StatusUnauthorized)
			return
		}
		torrentFile.WorkerID = torrentInput.WorkerID
	}

	// Create the torrent
	if err := db.Create(&torrentFile).Error; err != nil {
		http.Error(w, "Error creating torrent", http.StatusInternalServerError)
		return
	}

	// Create user torrent association
	userTorrent := UserTorrent{
		UserID:    userID,
		TorrentID: torrentFile.ID,
	}
	if err := db.Create(&userTorrent).Error; err != nil {
		http.Error(w, "Error creating user torrent association", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(torrentFile)
}

func getAllTorrents(w http.ResponseWriter, r *http.Request) {
	userID, _ := getUserIDFromToken(r)

	var torrents []Torrent
	if err := db.Joins("JOIN workers ON torrents.worker_id = workers.id").
		Where("workers.owner_id = ?", userID).
		Find(&torrents).Error; err != nil {
		http.Error(w, "Error fetching torrents", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(torrents)
	if err != nil {
		http.Error(w, "Something went wrong", http.StatusInternalServerError)
		return
	}
}

func watchStatus(w http.ResponseWriter, r *http.Request) {
	userID, _ := getUserIDFromToken(r)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	done := make(chan bool)
	go func() {
		<-r.Context().Done()
		done <- true
	}()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			var torrents []Torrent
			if err := db.Joins("JOIN workers ON torrents.worker_id = workers.id").
				Where("workers.owner_id = ?", userID).
				Find(&torrents).Error; err != nil {
				log.Printf("Error fetching torrents: %v", err)
				continue
			}

			data, err := json.Marshal(torrents)
			if err != nil {
				log.Printf("Error marshaling torrents: %v", err)
				continue
			}

			_, err = fmt.Fprintf(w, "data: %s\n\n", data)
			if err != nil {
				log.Printf("Error marshaling torrents: %v", err)
				continue
			}
			flusher.Flush()
		}
	}
}

func generateToken(userID uint) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(time.Hour * 24).Unix(),
	})

	return token.SignedString(jwtKey)
}

func getUserIDFromToken(r *http.Request) (uint, error) {
	tokenString := r.Header.Get("Authorization")
	if tokenString == "" {
		return 0, fmt.Errorf("no token provided")
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtKey, nil
	})

	if err != nil {
		return 0, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		userID := uint(claims["user_id"].(float64))
		return userID, nil
	}

	return 0, fmt.Errorf("invalid token")
}

func generateRegistrationToken() (string, error) {
	bytes := make([]byte, 32) // 256 bits
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func pollTorrents(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("authorization")
	if token == "" {
		http.Error(w, "Token is required", http.StatusBadRequest)
		return
	}

	var worker Worker
	if err := db.Where("registration_token = ?", token).First(&worker).Error; err != nil {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}

	var torrents []Torrent
	if err := db.Where("worker_id = ? AND status IN ?", worker.ID, []string{"queued", "exited"}).Find(&torrents).Error; err != nil {
		http.Error(w, "Error fetching torrents", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(torrents)
	if err != nil {
		http.Error(w, "Something went wrong", http.StatusInternalServerError)
		return
	}
}

func acknowledgeTorrent(w http.ResponseWriter, r *http.Request) {
	var data struct {
		Token     string `json:"token"`
		TorrentID uint   `json:"torrent_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var worker Worker
	if err := db.Where("registration_token = ?", data.Token).First(&worker).Error; err != nil {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}

	var torrentFile Torrent
	if err := db.First(&torrentFile, data.TorrentID).Error; err != nil {
		http.Error(w, "Torrent not found", http.StatusNotFound)
		return
	}

	torrentFile.Status = "downloading"
	if err := db.Save(&torrentFile).Error; err != nil {
		http.Error(w, "Error updating torrent status", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func updateTorrentProgress(w http.ResponseWriter, r *http.Request) {
	var data struct {
		Token               string  `json:"token"`
		TorrentID           uint    `json:"torrent_id"`
		PercentageCompleted float64 `json:"percentage_completed"`
		Status              string  `json:"status"`
		StreamURL           *string `json:"streamURL"`
		Files               []struct {
			Path           string `json:"path"`
			Sha1           string `json:"sha1"`
			BytesCompleted int64  `json:"bytes_completed"`
			Priority       int    `json:"priority"`
			TotalSize      int64  `json:"totalSize"`
		} `json:"files"`
	}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var worker Worker
	if err := db.Where("registration_token = ?", data.Token).First(&worker).Error; err != nil {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}

	var torrentFile Torrent
	if err := db.First(&torrentFile, data.TorrentID).Error; err != nil {
		http.Error(w, "Torrent not found", http.StatusNotFound)
		return
	}

	// Start a transaction
	tx := db.Begin()

	// Update torrent progress
	torrentFile.PercentageCompleted = data.PercentageCompleted
	if data.StreamURL != nil {
		torrentFile.StreamURL = data.StreamURL
	}
	if data.PercentageCompleted == 100 {
		torrentFile.Status = "completed"
	} else {
		torrentFile.Status = data.Status
	}
	if err := tx.Save(&torrentFile).Error; err != nil {
		tx.Rollback()
		http.Error(w, "Error updating torrent progress", http.StatusInternalServerError)
		return
	}

	// Update or create file information
	for _, file := range data.Files {
		var torrentFile TorrentFiles
		result := tx.Where(TorrentFiles{TorrentID: torrentFile.TorrentID, Path: file.Path}).FirstOrCreate(&torrentFile)
		if result.Error != nil {
			tx.Rollback()
			http.Error(w, "Error updating file information", http.StatusInternalServerError)
			return
		}

		torrentFile.Sha1 = file.Sha1
		torrentFile.BytesCompleted = file.BytesCompleted
		torrentFile.Priority = file.Priority
		torrentFile.TorrentID = data.TorrentID
		torrentFile.TotalSize = file.TotalSize

		if err := tx.Save(&torrentFile).Error; err != nil {
			tx.Rollback()
			http.Error(w, "Error saving file information", http.StatusInternalServerError)
			return
		}
	}

	// Commit the transaction
	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		http.Error(w, "Error committing transaction", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func initTorrentClient() error {
	config := torrent.NewDefaultClientConfig()
	config.NoUpload = false
	config.Seed = true
	config.NoDHT = false
	config.DisableUTP = false
	config.NoDefaultPortForwarding = true
	config.DataDir = os.TempDir()
	client_, err := torrent.NewClient(config)
	if err != nil {
		return err
	}

	client = client_
	return nil
}
