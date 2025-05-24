package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/Lavizord/checkers-server/config"
	"github.com/Lavizord/checkers-server/interfaces"
	"github.com/Lavizord/checkers-server/models"
	"github.com/Lavizord/checkers-server/postgrescli"
	"github.com/Lavizord/checkers-server/redisdb"

	"github.com/gorilla/mux"
)

var postgresClient *postgrescli.PostgresCli
var redisClient *redisdb.RedisClient
var name = "restapi"

func init() {
	config.LoadConfig()

	redisConData := config.Cfg.Redis
	client, err := redisdb.NewRedisClient(redisConData.Addr, redisConData.User, redisConData.Password)
	if err != nil {
		log.Fatalf("[%s-Redis] Error initializing Redis client: %v\n", name, err)
	}
	redisClient = client

	sqlcliente, err := postgrescli.NewPostgresCli(
		config.Cfg.Postgres.User,
		config.Cfg.Postgres.Password,
		config.Cfg.Postgres.DBName,
		config.Cfg.Postgres.Host,
		config.Cfg.Postgres.Port,
		config.Cfg.Postgres.Ssl,
	)
	if err != nil {
		log.Fatalf("[PostgreSQL] Error initializing POSTGRES client: %v\n", err)
	}
	postgresClient = sqlcliente
}

func gameLaunchHandler(w http.ResponseWriter, r *http.Request) {
	var req models.GameLaunchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithJSON(w, http.StatusBadRequest, models.GameLaunchResponse{
			Success: false,
			Message: "Invalid request body",
		})
		return
	}

	if req.GameID == "" || req.OperatorName == "" {
		respondWithJSON(w, http.StatusBadRequest, models.GameLaunchResponse{
			Success: false,
			Message: "Game ID and Operator Name are required",
		})
		return
	}

	//log.Printf("Received Game Launch Request: %+v", req)

	operator, err := postgresClient.FetchOperator(req.OperatorName, req.GameID)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, models.GameLaunchResponse{
			Success: false,
			Message: fmt.Sprintf("Invalid operator / gameID: %v", err),
		})
		return
	}

	if !operator.Active {
		respondWithJSON(w, http.StatusBadRequest, models.GameLaunchResponse{
			Success: false,
			Message: fmt.Sprintf("Inactive game for operator: %s", req.OperatorName),
		})
		return
	}

	module, exists := interfaces.OperatorModules[req.OperatorName]
	if !exists {
		respondWithJSON(w, http.StatusBadRequest, models.GameLaunchResponse{
			Success: false,
			Message: fmt.Sprintf("Unsupported operator: %s", req.OperatorName),
		})
		return
	}

	// Delegate the request to the module
	module.HandleGameLaunch(w, r, req, *operator, redisClient, postgresClient)
}

// Utility function to respond with JSON
func respondWithJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

func registerRoutes(r *mux.Router) {
	r.HandleFunc("/api/gamelaunch", gameLaunchHandler).Methods("POST")

	healthHandler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
	r.HandleFunc("/health", healthHandler).Methods("GET")
	r.HandleFunc("/api/health", healthHandler).Methods("GET")
}

func main() {
	defer func() {
		if redisClient != nil {
			redisClient.CloseRedisClient()
		}
		if postgresClient != nil {
			postgresClient.Close()
		}
	}()

	router := mux.NewRouter()
	registerRoutes(router)

	port := config.FirstPortFromConfig(name)
	addrs := fmt.Sprintf(":%d", port)

	log.Printf("[API] - HTTP server starting on %d...", port)
	log.Fatal(http.ListenAndServe(addrs, router))
}
