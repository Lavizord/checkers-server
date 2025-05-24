package main

import (
	"log"
	"time"

	"github.com/Lavizord/checkers-server/config"
	"github.com/Lavizord/checkers-server/messages"
	"github.com/Lavizord/checkers-server/redisdb"
)

var name = "BroadCastWorker"
var redisClient *redisdb.RedisClient

func init() {
	config.LoadConfig()
	redisConData := config.Cfg.Redis
	client, err := redisdb.NewRedisClient(redisConData.Addr, redisConData.User, redisConData.Password)
	if err != nil {
		log.Fatalf("[BroadcastWorker][Redis] Error initializing Redis client: %v", err)
	}
	redisClient = client
}

func main() {
	log.Printf("[BroadcastWorker] - Broadcasting room stats...\n")
	ticker := time.NewTicker(time.Duration(config.Cfg.Services["broadcastworker"].Timer) * time.Second)

	defer func() {
		ticker.Stop()
		if redisClient != nil {
			redisClient.CloseRedisClient()
		}
	}()
	for range ticker.C {
		msg, _ := messages.GenerateGameInfoMessageBytes(redisClient)
		// Publish the message
		err := redisClient.Publish("game_info", msg)
		if err != nil {
			log.Printf("[BroadcastWorker] Error publishing message: %v\n", err)
		} else {
			//log.Printf("[BroadcastWorker] Published room aggregates")
		}
	}
}
