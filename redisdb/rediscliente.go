package redisdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Lavizord/checkers-server/models"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	Client        *redis.Client
	Ctx           context.Context
	Subscriptions map[string]*redis.PubSub // Stores active subscriptions per channel
	mu            sync.Mutex
}

func NewRedisClient(addr string, username string, password string) (*RedisClient, error) {
	// Set up Redis client options
	options := &redis.Options{
		Addr: addr,
	}

	// If there's a username, set it in the options
	if username != "" {
		options.Username = username
	}

	// If there's a password, set it in the options
	if password != "" {
		options.Password = password
	}

	// Create the Redis client
	client := redis.NewClient(options)

	// Check the connection
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisClient] - failed to connect to Redis at %s: %w", addr, err)
	}

	return &RedisClient{
		Client:        client,
		Subscriptions: make(map[string]*redis.PubSub),
	}, nil
}

func (r *RedisClient) CloseRedisClient() {
	if r != nil {
		err := r.Client.Close()
		if err != nil {
			log.Fatalf("Error closing Redis connection: %v", err)
		}
	}
}

// RPush - Push serialized player to Redis
func (r *RedisClient) RPush(queue string, player *models.Player) error {
	data, err := json.Marshal(player)
	if err != nil {
		return err
	}
	return r.Client.RPush(context.Background(), queue, string(data)).Err()
}

// RPush - Push serialized player to Redis
func (r *RedisClient) RPushGeneric(queue string, data []byte) error {
	return r.Client.RPush(context.Background(), queue, string(data)).Err()
}

// BLPop - Retrieve a player from Redis queue
func (r *RedisClient) BLPop(queue string, timeoutSecond int) (*models.Player, error) {
	result, err := r.Client.BLPop(context.Background(), time.Duration(timeoutSecond)*time.Second, queue).Result()
	if err != nil {
		return nil, err
	}

	if len(result) > 1 {
		var player models.Player
		err = json.Unmarshal([]byte(result[1]), &player)
		if err != nil {
			return nil, err
		}
		return &player, nil
	}

	return nil, fmt.Errorf("no player found in queue")
}

// BLPop - Retrieve a player from Redis queue
func (r *RedisClient) BLPopGeneric(queue string, timeoutSecond int) ([]string, error) {
	result, err := r.Client.BLPop(context.Background(), time.Duration(timeoutSecond)*time.Second, queue).Result()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (rc *RedisClient) PublishPlayerEvent(player *models.Player, message string) error {

	err := rc.Client.Publish(context.Background(), GetPlayerPubSubChannel(*player), message).Err()
	if err != nil {
		return fmt.Errorf("[pkg/redisdb/cliente] - failed to publish player event: %w", err)
	}
	return nil
}

func (rc *RedisClient) Publish(channel string, message []byte) error {
	err := rc.Client.Publish(context.Background(), channel, message).Err()
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to publish message: %w", err)
	}
	return nil
}

func (r *RedisClient) SubscribePlayerChannel(player models.Player, messageHandler func(string)) {
	channel := GetPlayerPubSubChannel(player) // Function to get player-specific channel
	r.mu.Lock()
	if _, exists := r.Subscriptions[channel]; exists {
		r.mu.Unlock()
		fmt.Println("Already subscribed to", channel)
		return
	}
	pubsub := r.Client.Subscribe(context.Background(), channel)
	r.Subscriptions[channel] = pubsub
	r.mu.Unlock()

	go func() {
		for msg := range pubsub.Channel() {
			messageHandler(msg.Payload)
		}
	}()
}

func (r *RedisClient) Subscribe(channel string, messageHandler func(string)) {
	r.mu.Lock()
	if _, exists := r.Subscriptions[channel]; exists {
		r.mu.Unlock()
		log.Println("Already subscribed to", channel)
		return
	}
	pubsub := r.Client.Subscribe(context.Background(), channel)
	r.Subscriptions[channel] = pubsub
	r.mu.Unlock()

	go func() {
		for msg := range pubsub.Channel() {
			messageHandler(msg.Payload)
		}
	}()
}

func (r *RedisClient) UnsubscribePlayerChannel(player models.Player) {
	channel := GetPlayerPubSubChannel(player)

	r.mu.Lock()
	pubsub, exists := r.Subscriptions[channel]
	if !exists {
		r.mu.Unlock()
		log.Println("Not subscribed to", channel)
		return
	}
	delete(r.Subscriptions, channel)
	//log.Printf("[RedisClii] (UnsubscribePlayerChannel) - Deleted subscription of [%d] and [%v]\n", player.Name, channel)
	r.mu.Unlock()

	if err := pubsub.Unsubscribe(context.Background(), channel); err != nil {
		log.Println("Error unsubscribing from", channel, ":", err)
	} else {
		//log.Printf("[RedisClii] (UnsubscribePlayerChannel) - Unsubscribe of [%v] and [%v]\n", player.Name, channel)
	}
}

func (r *RedisClient) Unsubscribe(channel string) {
	r.mu.Lock()
	pubsub, exists := r.Subscriptions[channel]
	if !exists {
		r.mu.Unlock()
		log.Println("Not subscribed to", channel)
		return
	}
	delete(r.Subscriptions, channel)
	r.mu.Unlock()

	if err := pubsub.Unsubscribe(context.Background(), channel); err != nil {
		log.Println("Error unsubscribing from", channel, ":", err)
	}
}

func (r *RedisClient) PublishToPlayer(player models.Player, message string) error {
	return r.Client.Publish(context.Background(), GetPlayerPubSubChannel(player), message).Err()
}
func (r *RedisClient) PublishToPlayerID(playerID string, message string) error {
	return r.Client.Publish(context.Background(), "player:"+string(playerID), message).Err()
}
func (r *RedisClient) PublishToGamePlayer(player models.GamePlayer, message string) error {
	return r.Client.Publish(context.Background(), GetGamePlayerPubSubChannel(player), message).Err()
}
func (r *RedisClient) DisconnectPlayer(playerID string) {
	// Publish a disconnect message to Redis for that player
	message := fmt.Sprintf("disconnect:%s", playerID)
	r.PublishToPlayerID(playerID, message)
}

func (r *RedisClient) RemovePlayerFromQueue(queueName string, player *models.Player) error {
	ctx := context.Background()
	// Serialize the player to match stored format
	data, err := json.Marshal(player)
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to serialize player: %v", err)
	}
	// Remove the exact serialized JSON string from the queue
	removed, err := r.Client.LRem(ctx, queueName, 1, string(data)).Result()
	if err != nil {
		return fmt.Errorf("[RedisClient] - failed to remove player: %v", err)
	}
	if removed == 0 {
		return fmt.Errorf("[RedisClient] - player not found in queue %s", queueName)
	}

	return nil
}
