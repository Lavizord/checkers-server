package redisdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Lavizord/checkers-server/models"
)

func (r *RedisClient) AddGame(game *models.Game) error {
	data, err := json.Marshal(game)
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to serialize game: %v", err)
	}
	err = r.Client.HSet(context.Background(), "games", game.ID, data).Err()
	if err != nil {
		return err
	}
	betKey := fmt.Sprintf("games:bet:%.2f", game.BetValue)
	return r.Client.SAdd(context.Background(), betKey, game.ID).Err()
}

func (r *RedisClient) UpdateGame(game *models.Game) error {
	exists, err := r.GameExists(game.ID)
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to check game existence: %v", err)
	}
	if !exists {
		return fmt.Errorf("[RedisClient] - game with ID %s does not exist", game.ID)
	}

	data, err := json.Marshal(game)
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to serialize game: %v", err)
	}

	return r.Client.HSet(context.Background(), "games", game.ID, data).Err()
}

func (r *RedisClient) GetGame(gameID string) (*models.Game, error) {
	data, err := r.Client.HGet(context.Background(), "games", gameID).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisClient] - failed to get game: %v", err)
	}

	var game models.Game
	err = json.Unmarshal([]byte(data), &game)
	if err != nil {
		return nil, fmt.Errorf("[RedisClient] - failed to deserialize Game: %v", err)
	}
	return &game, nil
}

func (r *RedisClient) RemoveGame(gameID string) error {
	// First get the game to find its bet value
	data, err := r.Client.HGet(context.Background(), "games", gameID).Result()
	if err != nil {
		return fmt.Errorf("[RedisClient] - Error getting game: %s", gameID)
	}
	var game models.Game
	if err := json.Unmarshal([]byte(data), &game); err != nil {
		return fmt.Errorf("[RedisClient] - failed to unmarshal game: %v", err)
	}
	// Delete from main hash
	if err := r.Client.HDel(context.Background(), "games", gameID).Err(); err != nil {
		return fmt.Errorf("[RedisClient] - failed to delete game: %v", err)
	}
	// Remove from the specific bet value set
	betKey := fmt.Sprintf("games:bet:%.2f", game.BetValue)
	if err := r.Client.SRem(context.Background(), betKey, gameID).Err(); err != nil {
		return fmt.Errorf("[RedisClient] - failed to remove from bet value set: %v", err)
	}
	return nil
}

func (r *RedisClient) GameExists(gameID string) (bool, error) {
	exists, err := r.Client.HExists(context.Background(), "games", gameID).Result()
	return exists, err
}

func (r *RedisClient) GetNumberOfGames() int {
	// Use HLen to get the number of fields (games) in the "games" hash
	count, err := r.Client.HLen(context.Background(), "games").Result()
	if err != nil {
		log.Printf("[RedisClient] - failed to get number of games: %v", err)
		return 0
	}
	return int(count)
}

func (r *RedisClient) CountGamesByBetValue(betValue float64) (int64, error) {
	betKey := fmt.Sprintf("games:bet:%.2f", betValue)
	return r.Client.SCard(context.Background(), betKey).Result()
}

// This should be called when a disconnect happens during a game, it will save the
// session and some player data, to easily identify disconnected players.
func (r *RedisClient) SaveDisconnectSessionPlayerData(playerData models.Player, game models.Game) {
	if playerData.DisconnectedAt == 0 { // Check if it's unset
		playerData.DisconnectedAt = time.Now().Unix() // Set current timestamp
	}

	playerJSON, err := json.Marshal(playerData)
	if err != nil {
		log.Println("Error marshaling player:", err)
		return
	}
	key := fmt.Sprintf("players_disconnected:%s", playerData.SessionID)

	err = r.Client.Set(context.Background(), key, playerJSON, 0).Err()
	if err != nil {
		log.Println("Error saving player disconnection to Redis:", err)
		return
	}
	//fmt.Println("Player saved to disconnected list with key:", key)
}

// This should be called when a disconnect happens with a player in qeuey, it will save the
// session and some player data, to easily identify the players players.
func (r *RedisClient) SaveDisconnectInQueuePlayerData(playerData *models.Player) {
	if playerData.DisconnectedAt == 0 { // Check if it's unset
		playerData.DisconnectedAt = time.Now().Unix() // Set current timestamp
	}

	playerJSON, err := json.Marshal(playerData)
	if err != nil {
		log.Println("Error marshaling player:", err)
		return
	}
	key := fmt.Sprintf("players_disc_in_queue:%s", playerData.SessionID)

	err = r.Client.Set(context.Background(), key, playerJSON, 0).Err()
	if err != nil {
		log.Println("Error saving player disconnect in queue to Redis:", err)
		return
	}
	//fmt.Println("Player saved to disconnected list with key:", key)
}

// This retrieves our player disconnect, should be used to check if the player that just logged in is in a match.
func (r *RedisClient) GetDisconnectedPlayerData(sessionID string) *models.Player {
	key := fmt.Sprintf("players_disconnected:%s", sessionID)

	// Get the JSON data from Redis
	playerJSON, err := r.Client.Get(context.Background(), key).Result()
	if err != nil {
		log.Println("Error retrieving player disconnected from Redis:", err)
		return nil
	}

	// Unmarshal the JSON data into a Player struct
	var player models.Player
	err = json.Unmarshal([]byte(playerJSON), &player)
	if err != nil {
		log.Println("Error unmarshaling player disconnected JSON:", err)
		return nil
	}

	return &player
}

// This retrieves our player disconnect, should be used to check if the player that just logged in is in a match.
func (r *RedisClient) GetDisconnectedInQueuePlayerData(sessionID string) *models.Player {
	key := fmt.Sprintf("players_disc_in_queue:%s", sessionID)

	// Get the JSON data from Redis
	playerJSON, err := r.Client.Get(context.Background(), key).Result()
	if err != nil {
		log.Println("Error retrieving player disconnected in queuefrom Redis:", err)
		return nil
	}

	// Unmarshal the JSON data into a Player  struct
	var player models.Player
	err = json.Unmarshal([]byte(playerJSON), &player)
	if err != nil {
		log.Println("Error unmarshaling player disconnected in queue JSON:", err)
		return nil
	}

	return &player
}

func (r *RedisClient) DeleteDisconnectedInQueuePlayerData(sessionID string) error {
	key := fmt.Sprintf("players_disc_in_queue:%s", sessionID)

	// Delete the key from Redis
	err := r.Client.Del(context.Background(), key).Err()
	if err != nil {
		log.Println("Error deleting player disconnected in queue from Redis:", err)
		return err
	}

	//fmt.Println("Player session deleted with key:", key)
	return nil
}

func (r *RedisClient) DeleteDisconnectedPlayerSession(sessionID string) error {
	key := fmt.Sprintf("players_disconnected:%s", sessionID)

	// Delete the key from Redis
	err := r.Client.Del(context.Background(), key).Err()
	if err != nil {
		log.Println("Error deleting player disconnected from Redis:", err)
		return err
	}

	//fmt.Println("Player session deleted with key:", key)
	return nil
}
