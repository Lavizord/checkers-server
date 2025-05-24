package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Lavizord/checkers-server/messages"
	"github.com/Lavizord/checkers-server/models"
	"github.com/Lavizord/checkers-server/redisdb"
)

type QueueHandler struct {
	Client      *Client
	RedisClient *redisdb.RedisClient
	Msg         *messages.Message[json.RawMessage]

	// Track changes for cleanup
	initialValidationsFailed bool
	statusUpdated            bool
	addedToQueue             bool
	queueCountIncr           bool
}

func NewQueueHandler(client *Client, redis *redisdb.RedisClient, msg *messages.Message[json.RawMessage]) *QueueHandler {
	return &QueueHandler{
		Client:      client,
		RedisClient: redis,
		Msg:         msg,
	}
}

func (qh *QueueHandler) Process() {
	defer qh.cleanup()

	if !qh.validateStatusTransition() {
		qh.initialValidationsFailed = true
		log.Print("QueueHandler process invalid status transition.")
		return
	}

	betValue, err := qh.parseBetValue()
	if err != nil {
		qh.initialValidationsFailed = true
		log.Print("QueueHandler failed to parse bet.")
		return
	}

	found := false
	for _, v := range models.DamasValidBetAmounts {
		if v == betValue {
			found = true
			break
		}
	}
	if !found {
		qh.initialValidationsFailed = true
		log.Print("Bet is not valid for the configured ValidBetAmounts")
		return
	}

	qh.updatePlayerState(betValue)
	qh.addToRedisQueue()
	qh.updateQueueCount()
	qh.sendConfirmation()
}

func (qh *QueueHandler) cleanup() {
	var sendFailedQueueConfirmation = false

	if qh.initialValidationsFailed {
		qh.Client.player.UpdatePlayerStatus(models.StatusOnline)
		qh.RedisClient.UpdatePlayer(qh.Client.player)
		sendFailedQueueConfirmation = true
	}

	if qh.statusUpdated {
		qh.Client.player.UpdatePlayerStatus(models.StatusOnline)
		qh.RedisClient.UpdatePlayer(qh.Client.player)
		sendFailedQueueConfirmation = true
	}

	if qh.addedToQueue {
		queueName := fmt.Sprintf("queue:%f", qh.Client.player.SelectedBet)
		qh.RedisClient.Client.LRem(context.Background(), queueName, 1, qh.Client.player)
		sendFailedQueueConfirmation = true
	}

	if qh.queueCountIncr {
		qh.RedisClient.DecrementQueueCount(qh.Client.player.SelectedBet)
		sendFailedQueueConfirmation = true
	}

	if sendFailedQueueConfirmation {
		msg, _ := messages.GenerateQueueConfirmationMessage(false)
		qh.Client.send <- msg
	}
}

func (qh *QueueHandler) validateStatusTransition() bool {
	if qh.Client.player.UpdatePlayerStatus(models.StatusInQueue) != nil {
		msgBytes, _ := messages.GenerateGenericMessage("invalid", "Invalid status transition to 'queue'")
		qh.Client.send <- msgBytes
		return false
	}
	qh.statusUpdated = true
	return true
}

func (qh *QueueHandler) parseBetValue() (float64, error) {
	var betValue float64
	err := json.Unmarshal(qh.Msg.Value, &betValue)
	if err != nil {
		log.Printf("Error determining player bet value: %v\n", err)
		msgBytes, _ := messages.GenerateGenericMessage("error", "Error determining player bet value")
		qh.Client.send <- msgBytes
		return 0, err
	}
	return betValue, nil
}

func (qh *QueueHandler) updatePlayerState(betValue float64) {
	qh.Client.player.SelectedBet = betValue
	qh.Client.player.Status = models.StatusInQueue
	//qh.RedisClient.UpdatePlayersInQueueSet(qh.Client.player.ID, models.StatusInQueue)
	qh.RedisClient.UpdatePlayer(qh.Client.player)
}

func (qh *QueueHandler) addToRedisQueue() error {
	queueName := fmt.Sprintf("queue:%f", qh.Client.player.SelectedBet)
	err := qh.RedisClient.RPush(queueName, qh.Client.player)
	if err != nil {
		log.Printf("Error pushing player to Redis queue: %v\n", err)
		msgBytes, _ := messages.GenerateGenericMessage("error", "error adding player to queue")
		qh.Client.send <- msgBytes
		return err
	}
	qh.addedToQueue = true
	return nil
}

func (qh *QueueHandler) updateQueueCount() {
	exists, err := qh.RedisClient.CheckQueueCountExists(qh.Client.player.SelectedBet)
	if err == nil {
		if !exists {
			qh.RedisClient.CreateQueueCount(qh.Client.player.SelectedBet)
		} else {
			qh.RedisClient.IncrementQueueCount(qh.Client.player.SelectedBet)
		}
		qh.queueCountIncr = true
	}
}

func (qh *QueueHandler) sendConfirmation() {
	m, err := messages.GenerateQueueConfirmationMessage(true)
	if err != nil {
		fmt.Println("Error generating queue confirmation:", err)
		msgBytes, _ := messages.GenerateGenericMessage("error", "error generating confirmation")
		qh.Client.send <- msgBytes
		return
	}
	qh.Client.send <- m

	// Success - disable cleanup
	qh.statusUpdated = false
	qh.addedToQueue = false
	qh.queueCountIncr = false
}
