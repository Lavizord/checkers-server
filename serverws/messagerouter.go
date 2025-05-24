package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Lavizord/checkers-server/messages"

	"github.com/Lavizord/checkers-server/models"
	"github.com/Lavizord/checkers-server/redisdb"
)

// Directly route to the right handler based on the command.
//
// The valid command sent will be processsed and router to the right handler. Some of those handlers
// might send the message to redis.
func RouteMessages(message *messages.Message[json.RawMessage], client *Client, redis *redisdb.RedisClient) {
	switch message.Command {
	case "queue":
		handleQueue(message, client, redis)
		return

	case "leave_queue":
		handleLeaveQueue(client, redis)
		return

	case "ready_queue":
		handleReadyQueue(message, client, redis)
		return

	case "leave_room":
		if client.player.Status != models.StatusInRoom && client.player.Status != models.StatusInRoomReady {
			msg, _ := messages.GenerateGenericMessage("invalid", "Can't issue a leave room when not in a Room.")
			client.send <- msg
			return
		}
		handleLeaveRoom(client, redis)
		return

	case "leave_game":
		if client.player.Status != models.StatusInGame {
			msg, _ := messages.GenerateGenericMessage("invalid", "Can't issue a leave game when not in a game.")
			client.send <- msg
			return
		}
		handleLeaveGame(client, redis)
		return

	case "move_piece":
		if client.player.Status != models.StatusInGame {
			msg, _ := messages.GenerateGenericMessage("invalid", "Can't issue a move when not in a Game.")
			client.send <- msg
			return
		}
		handleMovePiece(message, client, redis)
		return
	}
}

func handleQueue(msg *messages.Message[json.RawMessage], client *Client, redis *redisdb.RedisClient) {
	qh := NewQueueHandler(client, redis, msg)
	qh.Process()
}

func handleLeaveQueue(client *Client, redis *redisdb.RedisClient) {
	if client.player.UpdatePlayerStatus(models.StatusOnline) != nil {
		msg, _ := messages.GenerateGenericMessage("invalid", "Invalid status transition to 'Online'")
		client.send <- msg
		return
	}
	client.player.SetStatusOnline()
	redis.UpdatePlayer(client.player) // This is important, we will only re-add players to a queue that are in queue.
	//redis.UpdatePlayersInQueueSet(client.player.ID, models.StatusOnline)
	queueName := fmt.Sprintf("queue:%f", client.player.SelectedBet)
	err := redis.RemovePlayerFromQueue(queueName, client.player)
	if err != nil {
		//log.Printf("Error removing player from Redis queue: %v\n", err)	//! This was commented, it fails when there is only 1 player in queue.
		//client.send <- []byte("Error removing player to queue")
		return
	}
	// we update out player status., send a confirmation message back to the player
	m, err := messages.GenerateQueueConfirmationMessage(false)
	if err != nil {
		msg, _ := messages.GenerateGenericMessage("error", "Error generating queue confirmation false:"+err.Error())
		log.Println("Error generating queue confirmation false:", err)
		client.send <- msg
		return
	}
	client.send <- m
}

func handleReadyQueue(msg *messages.Message[json.RawMessage], client *Client, redis *redisdb.RedisClient) {
	// We have to check if the message for readyqueue true or false.
	var value bool
	json.Unmarshal(msg.Value, &value)
	if value {
		// update the player status to ready / awaiting opponent.
		if client.player.UpdatePlayerStatus(models.StatusInRoomReady) != nil {
			msg, _ := messages.GenerateGenericMessage("invalid", "Invalid status transition to 'ready_queue true'")
			client.send <- msg
			return
		}
	} else {
		// update the player status, to unready / waiting ready.
		if client.player.UpdatePlayerStatus(models.StatusInRoom) != nil {
			msg, _ := messages.GenerateGenericMessage("invalid", "Invalid status transition to 'ready_queue false'")
			client.send <- msg
			return
		}
	}
	msgBytes, _ := messages.GenerateGenericMessage("info", "Processing 'ready_queue'")
	client.send <- msgBytes
	// we update our player to redis.
	//redis.UpdatePlayersInQueueSet(client.player.ID, client.player.Status)
	err := redis.UpdatePlayer(client.player)
	if err != nil {
		log.Printf("Error updating player to Redis: %v\n", err)
		msgBytes, _ := messages.GenerateGenericMessage("error", "Error Updating player: "+err.Error())
		client.send <- msgBytes
		return
	}
	err = redis.RPush("ready_queue", client.player) // now we tell roomworker to process this player ready.
	if err != nil {
		log.Printf("Error pushing player to Redis ready queue: %v\n", err)
		msgBytes, _ := messages.GenerateGenericMessage("error", "Pushing player to queue: "+err.Error())
		client.send <- msgBytes
		return
	}
}

func handleLeaveRoom(client *Client, redis *redisdb.RedisClient) {
	// update the player status
	if client.player.UpdatePlayerStatus(models.StatusOnline) != nil {
		msgBytes, _ := messages.GenerateGenericMessage("invalid", "Invalid status transition to 'leave_room'")
		client.send <- msgBytes
		return
	}
	msgBytes, _ := messages.GenerateGenericMessage("info", "Processing 'leave_room'")
	client.send <- msgBytes
	// we update our player to redis.
	err := redis.UpdatePlayer(client.player)
	if err != nil {
		log.Printf("Error updagint player to Redis on HandleLeaveRoom: %v\n", err)
		msg, _ := messages.GenerateGenericMessage("error", "error updating player.")
		client.send <- msg
		return
	}
	err = redis.RPush("leave_room", client.player)
	if err != nil {
		log.Printf("Error pushing player to Redis leave_room queue: %v\n", err)
		msg, _ := messages.GenerateGenericMessage("error", "error leaving room.")
		client.send <- msg
		return
	}
}

func handleLeaveGame(client *Client, redis *redisdb.RedisClient) {
	msgBytes, _ := messages.GenerateGenericMessage("invalid", "Processing 'leave_game'")
	client.send <- msgBytes
	err := redis.RPush("leave_game", client.player)
	if err != nil {
		log.Printf("Error pushing player to Redis leave_game queue: %v\n", err)
		msgBytes, _ := messages.GenerateGenericMessage("error", "Error adding player to leave_game")
		client.send <- msgBytes
		return
	}
}

func handleMovePiece(message *messages.Message[json.RawMessage], client *Client, redis *redisdb.RedisClient) {
	var move models.Move
	err := json.Unmarshal([]byte(message.Value), &move)
	if err != nil {
		log.Printf("[Handlers] - Handle Move Piece - JSON Unmarshal Error: %v\n", err)
		msg, _ := messages.GenerateGenericMessage("invalid", "Handle Move Piece - JSON Unmarshal Error.")
		client.send <- msg
		return
	}
	if move.PlayerID != client.player.ID {
		log.Printf("[Handlers] - Handle Move Piece - move.PlayerID != player.ID\n")
		msg, _ := messages.GenerateGenericMessage("invalid", "Handle Move Piece - move.PlayerID != player.ID.")
		client.send <- msg
		return
	}
	// movement message is sent to the game worker
	err = redis.RPushGeneric("move_piece", message.Value)
	if err != nil {
		log.Printf("Error pushing move to Redis handleMovePiece queue: %v\n", err)
		msg, _ := messages.GenerateGenericMessage("error", "error pushing move to gameworker.")
		client.send <- msg
		return
	}
}
