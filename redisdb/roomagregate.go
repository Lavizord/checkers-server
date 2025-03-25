package redisdb

import (
	"checkers-server/models"
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
)

func (r *RedisClient) CreateRoomAggregate(aggregateValue float64) {
	key := fmt.Sprintf("RoomAgreagate:%f", aggregateValue)
	_, err := r.Client.SetNX(context.Background(), key, 1, 0).Result()
	if err != nil {
		log.Printf("Error setting room aggregate: %v", err)
	}
}

func (r *RedisClient) IncrementRoomAggregate(aggregateValue float64) {
	key := fmt.Sprintf("RoomAgreagate:%f", aggregateValue)
	_, err := r.Client.Incr(context.Background(), key).Result()
	if err != nil {
		log.Printf("Error incrementing room aggregate: %v", err)
	}
}

func (r *RedisClient) DecrementRoomAggregate(aggregateValue float64) {
	key := fmt.Sprintf("RoomAgreagate:%f", aggregateValue)
	_, err := r.Client.Decr(context.Background(), key).Result()
	if err != nil {
		log.Printf("Error decrementing room aggregate: %v", err)
	}
}

func (r *RedisClient) CheckRoomAggregateExists(aggregateValue float64) (bool, error) {
	key := fmt.Sprintf("RoomAgreagate:%f", aggregateValue)

	// Check if the key exists in Redis
	exists, err := r.Client.Exists(context.Background(), key).Result()
	if err != nil {
		return false, fmt.Errorf("Error checking if room aggregate exists: %v", err)
	}
	return exists == 1, nil
}

// !TODO : Terminar isto.
func (r *RedisClient) GetRoomAggregates() (*models.RoomAggregateResponse, error) {
	keys, err := r.Client.Keys(context.Background(), "RoomAgreagate:*").Result()
	if err != nil {
		return nil, fmt.Errorf("Error retrieving room aggregates: %v", err)
	}
	var playerCount []models.PlayerCountPerBetValue
	//var totalPlayers int64
	//totalPlayers, err = r.GetNumPlayersInQueue()
	for _, key := range keys {
		parts := strings.Split(key, ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("Invalid key format: %s", key)
		}
		aggregateValue, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return nil, fmt.Errorf("Error parsing aggregate value from key %s: %v", key, err)
		}
		value, err := r.Client.Get(context.Background(), key).Int64()
		if err != nil {
			return nil, fmt.Errorf("Error getting value for key %s: %v", key, err)
		}

		aggregate := models.PlayerCountPerBetValue{
			BetValue:    aggregateValue, // The numeric part after the colon
			PlayerCount: value,
		}
		playerCount = append(playerCount, aggregate)
		//totalPlayers += int(value)
	}

	// Sort by Count in descending order (most players first)
	sort.Slice(playerCount, func(i, j int) bool {
		return playerCount[i].PlayerCount > playerCount[j].PlayerCount
	})

	return &models.RoomAggregateResponse{
		RoomAggregate: playerCount,
	}, nil
}
