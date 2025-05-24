package redisdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Lavizord/checkers-server/models"

	"github.com/redis/go-redis/v9"
)

func (r *RedisClient) AddRoom(room *models.Room) error {
	ctx := context.Background()
	// Serialize the full room object
	data, err := json.Marshal(room)
	if err != nil {
		return fmt.Errorf("[RedisClient] (Room) - failed to serialize room: %v", err)
	}
	// Store full room data in a hash
	roomKey := fmt.Sprintf("room:%s", room.ID)
	err = r.Client.HSet(ctx, roomKey, map[string]interface{}{
		"id":       room.ID,
		"BetValue": room.BetValue,
		"data":     string(data), // Store full room JSON
	}).Err()
	if err != nil {
		return fmt.Errorf("[RedisClient] (Room) - failed to store room data: %v", err)
	}
	// Add room ID to sorted set, indexed by bid amount, this will help us get the rooms by bid amount
	zsetKey := "rooms_by_bid"
	err = r.Client.ZAdd(ctx, zsetKey, redis.Z{
		Score:  room.BetValue,
		Member: room.ID,
	}).Err()
	if err != nil {
		return fmt.Errorf("[RedisClient] (Room) - failed to update room index: %v", err)
	}
	// Manage Queue Count
	exists, err := r.CheckQueueCountExists(room.BetValue)
	if err == nil {
		if !exists {
			r.CreateQueueCount(room.BetValue)
		}
	}
	return nil
}

func (r *RedisClient) RemoveRoom(key string) error {
	ctx := context.Background()
	// Check if room exists correctly
	exists, err := r.Client.HExists(ctx, key, "id").Result()
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to check if room exists: %v", err)
	}
	if !exists {
		return fmt.Errorf("[RedisClient] - attempting to delete room that does not exist: %s", key)
	}
	// Delete the entire room hash
	if err := r.Client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("[RedisClient] - failed to delete room: %v", err)
	}
	return nil
}

func (r *RedisClient) GetRoomByID(roomID string) (*models.Room, error) {
	ctx := context.Background()
	roomKey := fmt.Sprintf("room:%s", roomID)
	data, err := r.Client.HGet(ctx, roomKey, "data").Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisClient] - failed to retrieve room %s: %v", roomID, err)
	}

	var room models.Room
	if err := json.Unmarshal([]byte(data), &room); err != nil {
		return nil, fmt.Errorf("[RedisClient] - failed to unmarshal room data: %v", err)
	}
	return &room, nil
}

func (r *RedisClient) GetRoomsByBetValue(BetValue float64) ([]models.Room, error) {
	ctx := context.Background()
	zsetKey := "rooms_by_bid"
	// Get room IDs in the given bid amount range
	roomIDs, err := r.Client.ZRangeByScore(ctx, zsetKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", BetValue),
		Max: fmt.Sprintf("%f", BetValue),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisClient] - failed to retrieve rooms: %v", err)
	}
	// Retrieve full room data from Hash
	var rooms []models.Room
	for _, roomID := range roomIDs {
		roomKey := fmt.Sprintf("room:%s", roomID)
		data, err := r.Client.HGet(ctx, roomKey, "data").Result()
		if err != nil {
			continue // Skip if the room data is missing
		}
		var room models.Room
		if err := json.Unmarshal([]byte(data), &room); err == nil {
			rooms = append(rooms, room)
		}
	}
	return rooms, nil
}

func (r *RedisClient) GetEmptyRoomsByBetValue(BetValue float64) ([]models.Room, error) {
	ctx := context.Background()
	zsetKey := "rooms_by_bid"
	// Get room IDs in the given bid amount range
	roomIDs, err := r.Client.ZRangeByScore(ctx, zsetKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", BetValue),
		Max: fmt.Sprintf("%f", BetValue),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisClient] - failed to retrieve rooms: %v", err)
	}
	// Retrieve full room data from Hash
	var rooms []models.Room
	for _, roomID := range roomIDs {
		roomKey := fmt.Sprintf("room:%s", roomID)
		data, err := r.Client.HGet(ctx, roomKey, "data").Result()
		if err != nil {
			continue // Skip if the room data is missing
		}

		var room models.Room
		if err := json.Unmarshal([]byte(data), &room); err == nil {
			if room.Player2 == nil {
				rooms = append(rooms, room)
			}
		}
	}
	return rooms, nil
}

func (r *RedisClient) PublishToRoomPubSub(roomID string, message string) error {
	err := r.Client.Publish(context.Background(), "roompubsub:"+roomID, message).Err()
	if err != nil {
		log.Println("Publish error:", err)
		return err
	}
	return nil
}
