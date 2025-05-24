package redisdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Lavizord/checkers-server/models"
)

func (r *RedisClient) AddPlayer(player *models.Player) error {
	data, err := json.Marshal(player)
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to serialize player: %v", err)
	}
	log.Println("Adding player back to redis")
	// Uses a shared key ("players") , store the player data under their ID
	return r.Client.HSet(context.Background(), "players", player.ID, data).Err()
}

func (r *RedisClient) UpdatePlayer(player *models.Player) error {
	ctx := context.Background()
	exists, err := r.Client.HExists(ctx, "players", player.ID).Result()
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to check player existence: %v", err)
	}
	if !exists {
		return fmt.Errorf("[RedisClient] - player with ID %s does not exist", player.ID)
	}

	data, err := json.Marshal(player)
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to serialize game: %v", err)
	}

	return r.Client.HSet(ctx, "players", player.ID, data).Err()
}

func (r *RedisClient) GetPlayer(playerID string) (*models.Player, error) {
	data, err := r.Client.HGet(context.Background(), "players", playerID).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisClient] - failed to get player: %v", err)
	}

	var player models.Player
	err = json.Unmarshal([]byte(data), &player)
	if err != nil {
		return nil, fmt.Errorf("[RedisClient] - failed to deserialize player: %v", err)
	}
	return &player, nil
}

func (r *RedisClient) GetNumPlayers() (int64, error) {
	// Use HLen to get the number of fields (players) in the "players" hash
	numPlayers, err := r.Client.HLen(context.Background(), "players").Result()
	if err != nil {
		return 0, fmt.Errorf("[RedisClient] - failed to get number of players: %v", err)
	}
	return numPlayers, nil
}

func (r *RedisClient) RemovePlayer(playerID string) error {
	exists, err := r.Client.HExists(context.Background(), "players", playerID).Result()
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to check if player exists: %v", err)
	}

	if !exists {
		return fmt.Errorf("[RedisClient] - attempting to delete player that does not exist: %s", playerID)
	}
	return r.Client.HDel(context.Background(), "players", playerID).Err()
}

// func (r *RedisClient) UpdatePlayersInQueueSet(playerID string, newStatus models.PlayerStatus) error {
// 	// Update the "players_in_queue" set based on the new status
// 	if newStatus == models.StatusInQueue {
// 		// Add player to the queue set
// 		err := r.Client.SAdd(context.Background(), "players_in_queue", playerID).Err()
// 		if err != nil {
// 			return fmt.Errorf("[RedisClient] - failed to add player to players_in_queue set: %v", err)
// 		}
// 	} else {
// 		// Remove player from the queue set
// 		err := r.Client.SRem(context.Background(), "players_in_queue", playerID).Err()
// 		if err != nil {
// 			return fmt.Errorf("[RedisClient] - failed to remove player from players_in_queue set: %v", err)
// 		}
// 	}

// 	return nil
// }

// func (r *RedisClient) GetNumPlayersInQueue() (int64, error) {
// 	// Use SCARD to get the number of players in the "players_in_queue" set
// 	numPlayers, err := r.Client.SCard(context.Background(), "players_in_queue").Result()
// 	if err != nil {
// 		return 0, fmt.Errorf("[RedisClient] - failed to get number of players in queue: %v", err)
// 	}
// 	return numPlayers, nil
// }
