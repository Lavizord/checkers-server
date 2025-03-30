package main

import (
	"checkers-server/config"
	"checkers-server/interfaces"
	"checkers-server/messages"
	"checkers-server/models"
	"checkers-server/postgrescli"
	"checkers-server/redisdb"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

var pid int
var redisClient *redisdb.RedisClient
var postgresClient *postgrescli.PostgresCli
var name = "roomworker"

func init() {
	pid = os.Getpid()
	config.LoadConfig()
	redisAddr := config.Cfg.Redis.Addr
	client, err := redisdb.NewRedisClient(redisAddr)
	if err != nil {
		log.Fatalf("[Redis] Error initializing Redis client: %v\n", err)
	}
	redisClient = client

	sqlcliente, err := postgrescli.NewPostgresCli(
		config.Cfg.Postgres.User,
		config.Cfg.Postgres.Password,
		config.Cfg.Postgres.DBName,
		config.Cfg.Postgres.Host,
		config.Cfg.Postgres.Port,
	)
	if err != nil {
		log.Fatalf("[%s-PostgreSQL] Error initializing POSTGRES client: %v\n", name, err)
	}
	postgresClient = sqlcliente
}

func main() {
	log.Printf("[RoomWorker-%d] - Waiting for room messages...\n", pid)

	go processReadyQueue()
	go processRoomEnding()
	go processQueue()
	select {}
}

func processRoomCreation() {
	for {
		playerData, err := redisClient.BLPop("create_room", 0) // Block
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error retrieving player:%v\n", pid, err)
			continue
		}
		log.Printf("[RoomWorker-%d] - create room!: %+v\n", pid, playerData)
		handleCreateRoom(playerData)
	}
}

func processRoomJoin() {
	for {
		playerData, err := redisClient.BLPop("join_room", 0) // Block
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error retrieving player:%v\n", pid, err)
			continue
		}
		log.Printf("[RoomWorker-%d] - processing join room!: %+v\n", pid, playerData)
		handleJoinRoom(playerData)
	}
}

func processQueue() {
	// Launch a goroutine for each bet queue
	for _, bet := range models.DamasValidBetAmounts {
		go processQueueForBet(bet)
	}

	// Block forever or wait on a channel (to prevent the main goroutine from exiting)
	select {}
}

func processQueueForBet(bet float64) {
	queueName := fmt.Sprintf("queue:%f", bet)
	for {
		// Block indefinitely for player1 (this goroutine is dedicated to this queue)
		player1, err := redisClient.BLPop(queueName, 0)
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error retrieving player 1 from %s: %v\n", pid, queueName, err)
			continue
		}
		log.Printf("[RoomWorker-%d] - Retrieved player 1 from %s: %v\n", pid, queueName, player1)

		player1Details, err := redisClient.GetPlayer(player1.ID)
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error retrieving player 1 details, player removed from queue: %v\n", pid, err)
			redisClient.DecrementQueueCount(bet)
			continue
		}
		// we check to see if the player is eligible to be processed.
		if !player1Details.IsEligibleForQueue() {
			log.Printf("[RoomWorker-%d] - player1 not eligible to be processed by the queue, player removed from queue: %v\n", pid, queueName)
			redisClient.DecrementQueueCount(bet)
			continue
		}

		// Try fetching the second player with a timeout
		player2, err := redisClient.BLPop(queueName, 5)
		if err != nil || player2 == nil {
			log.Printf("[RoomWorker-%d] - No second player found in %s, re-queueing player 1.\n", pid, queueName)
			// Since we failed to get the player2, we will requeue the player1.
			time.Sleep(time.Second * 3)
			redisClient.RPush(queueName, player1)
			continue
		}
		log.Printf("[RoomWorker-%d] - Retrieved player 2 from %s: %v\n", pid, queueName, player2)

		player2Details, err := redisClient.GetPlayer(player2.ID)

		if player1Details.ID == player2Details.ID {
			log.Printf("[RoomWorker-%d] - player1Details.ID == player2Details.ID, player2 removed from queue: %v\n", pid, queueName)
			redisClient.RPush(queueName, player1)
			redisClient.DecrementQueueCount(bet)
			continue
		}

		// before we handle the paired, we will do a final check to make sure the players2 is still online / valid.
		if err != nil || !player2Details.IsEligibleForQueue() {
			// If it is not valid, we will add player 1 back to the queue.
			log.Printf("[RoomWorker-%d] - player2 not eligible to be processed by the queue, player removed from queue: %v\n", pid, queueName)
			time.Sleep(time.Second * 3)
			redisClient.RPush(queueName, player1)
			redisClient.DecrementQueueCount(bet)
			continue
		}

		// Process both players
		log.Printf("[RoomWorker-%d] - Pairing players: %s and %s from %s\n", pid, player1, player2, queueName)
		handleQueuePaired(player1, player2)
	}
}

func processReadyQueue() {
	for {
		playerData, err := redisClient.BLPop("ready_queue", 0) // Block
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error retrieving player:%v\n", pid, err)
			continue
		}
		log.Printf("[RoomWorker-%d] - processing ready room!: %+v\n", pid, playerData)
		// Aqui ou damos handle do ready queue ou handle do unreadyqueue
		if playerData.Status == models.StatusInRoomReady {
			handleReadyQueue(playerData)
		}
		if playerData.Status == models.StatusInRoom {
			handleUnReadyQueue(playerData)
		}
	}
}

func processRoomEnding() {
	for {
		playerData, err := redisClient.BLPop("leave_room", 0)
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error retrieving player:%v\n", pid, err)
			continue
		}
		log.Printf("[RoomWorker-%d] - Processing the end of room: %+v\n", pid, playerData)
		room, err := redisClient.GetRoomByID(playerData.RoomID)
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error retrieving room:%v\n", pid, err)
			continue
		}
		player2ID, err := room.GetOpponentPlayerID(playerData.ID)
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error retrieving opponent id:%v\n", pid, err)
			continue
		}
		player2, err := redisClient.GetPlayer(player2ID)
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error retrieving opponent player:%v\n", pid, err)
			continue
		}
		msg, err := messages.NewMessage("opponent_left_room", true)
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error generating message:%v\n", pid, err)
			continue
		}
		redisClient.PublishToPlayer(*player2, string(msg))
		addPlayerToQueue(player2)
		// Reset both player data.
		playerData.SetStatusOnline()
		redisClient.UpdatePlayer(playerData)
		err = redisClient.RemoveRoom(redisdb.GenerateRoomRedisKeyById(room.ID))
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error removing room: %v\n", pid, err)
			continue
		}
		redisClient.DecrementQueueCount(playerData.SelectedBet)
		log.Printf("[RoomWorker-%d] - End of room ending: %v\n", pid, err)
	}
}

func handleQueuePaired(player1, player2 *models.Player) {
	log.Printf("[RoomWorker-%d] - Handling player1 (CREATE ROOM): %s (Session: %s, Currency: %s)\n",
		pid, player1.ID, player1.SessionID, player1.Currency)
	log.Printf("[RoomWorker-%d] - Handling player2 (CREATE ROOM): %s (Session: %s, Currency: %s)\n",
		pid, player2.ID, player2.SessionID, player2.Currency)

	room := &models.Room{
		ID:                 models.GenerateUUID(),
		Player1:            player1,
		Player2:            player2,
		StartDate:          time.Now(),
		Currency:           player1.Currency,
		BetValue:           player1.SelectedBet,
		OperatorIdentifier: player1.OperatorIdentifier,
	}

	player1.RoomID = room.ID
	player2.RoomID = room.ID
	player1.Status = models.StatusInRoom
	player2.Status = models.StatusInRoom
	redisClient.UpdatePlayer(player1)
	redisClient.UpdatePlayer(player2)
	// Now we set the player colors.
	colorp1 := rand.Intn(2)
	colorp2 := 1
	if colorp1 == 1 {
		room.CurrentPlayerID = player1.ID
		colorp2 = 0
	} else {
		room.CurrentPlayerID = player2.ID
	}
	err := redisClient.AddRoom(room)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Failed to add room to Redis: %v\n", pid, err)
		return
	}
	message1, err := messages.GeneratePairedMessage(room.Player1, room.Player2, room.ID, colorp1)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handling paired message1 for p1: %s\n", pid, err)
		return
	}
	message2, err2 := messages.GeneratePairedMessage(room.Player2, room.Player1, room.ID, colorp2)
	if err2 != nil {
		log.Printf("[RoomWorker-%d] - Error handling paired message1 for p2:%s\n", pid, err2)
		return
	}
	log.Printf("[RoomWorker-%d] - Handling player (JOIN ROOM) - Message1 for player1: %s\n", pid, message1)
	log.Printf("[RoomWorker-%d] - Handling player (JOIN ROOM) - Message2 for player2: %s\n", pid, message2)
	// Publish the validated message to Redis
	err = redisClient.PublishToPlayer(*player1, string(message1))
	if err != nil {
		log.Printf("[RoomWorker-%d] - Failed to publish message1 to player1: %v\n", pid, err)
		return
	}
	err = redisClient.PublishToPlayer(*player2, string(message2))
	if err != nil {
		log.Printf("[RoomWorker-%d] - Failed to publish message2 to player2: %v\n", pid, err)
		return
	}
	log.Printf("[RoomWorker-%d] - Player successfully handled and notified, of room pairing.\n", pid)
	// We decrement it twice to account for both players starting a match.
	redisClient.DecrementQueueCount(player1.SelectedBet)
	redisClient.DecrementQueueCount(player1.SelectedBet)
}

func handleReadyQueue(player *models.Player) {
	log.Printf("[RoomWorker-%d] - Handling player (READY QUEUE): %s (Session: %s, Currency: %s)\n",
		pid, player.ID, player.SessionID, player.Currency)

	proom, err := redisClient.GetRoomByID(player.RoomID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleReadyQueue getting player room:%s\n", pid, err)
		return
	}
	player2ID, err := proom.GetOpponentPlayerID(player.ID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleReadyQueue getting player opponent ID:%s\n", pid, err)
		return
	}
	player2, err := redisClient.GetPlayer(player2ID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleReadyQueue getting opponent player:%s\n", pid, err)
		return
	}
	// We will always notify the opponent the we are ready.
	msg, err := messages.GenerateOpponentReadyMessage(true)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleReadyQueue getting GenerateOpponentReadyMessage(true) for opponent:%s\n", pid, err)
		return
	}
	redisClient.PublishPlayerEvent(player2, string(msg))
	// now we tell our player that is ready if the opponent is ready or not.
	if player2.Status != models.StatusInRoomReady {
		log.Printf("[RoomWorker-%d] - handleReadyQueue Opponent aint ready yet!:%s\n", pid, err)
		msg, err := messages.GenerateOpponentReadyMessage(false)
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error handleReadyQueue getting GenerateOpponentReadyMessage(false) for player:%s\n", pid, err)
		}
		redisClient.PublishPlayerEvent(player, string(msg))
		return
	}
	// Now! If both players are ready...!!
	// Before we start the game, we will need to post to the wallet api of the bet, we will use our api interface for that.
	module, exists := interfaces.OperatorModules[proom.OperatorIdentifier.OperatorName]
	if !exists {
		log.Printf("[RoomWorker-%d] - Error handleReadyQueue getting getting interfaces.OperatorModules[%v]:%s\n", pid, proom.OperatorIdentifier.OperatorName, err)
		return
	}
	session1, err := redisClient.GetSessionByID(player.SessionID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleReadyQueue fetching player1 sessionID:%s\n", pid, err)
		return
	}
	session2, err := redisClient.GetSessionByID(player2.SessionID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleReadyQueue fetching player2 sessionID:%s\n", pid, err)
		return
	}

	newBalance1, err := module.HandlePostBet(postgresClient, redisClient, *session1, int64(proom.BetValue*100), proom.ID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error HandlePostBet failed to bet:%s for sessionid:[%s]\n", pid, err, session1.ID)
		player.SetStatusOnline()
		redisClient.UpdatePlayer(player)
		msg, _ := messages.GenerateGenericMessage("error", err.Error())
		redisClient.PublishPlayerEvent(player, string(msg))
		redisClient.RemoveRoom(redisdb.GenerateRoomRedisKeyById(proom.ID))

		msg, _ = messages.NewMessage("opponent_left_room", true)
		redisClient.PublishPlayerEvent(player2, string(msg))

		// since the first player failed the api check, we will queue up the second plyer.
		addPlayerToQueue(player2)
		// TODO: CREDITAR VALOR A JOGADOR.
		return
	}
	newBalance2, err := module.HandlePostBet(postgresClient, redisClient, *session2, int64(proom.BetValue*100), proom.ID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error HandlePostBet failed to bet:%s for sessionid:[%s]\n", pid, err, session1.ID)
		player2.SetStatusOnline()
		redisClient.UpdatePlayer(player2)
		msg, _ := messages.GenerateGenericMessage("error", err.Error())
		redisClient.PublishPlayerEvent(player2, string(msg))
		redisClient.RemoveRoom(redisdb.GenerateRoomRedisKeyById(proom.ID))

		// since the second player failed the api check, we will queue up the first player.
		addPlayerToQueue(player)
		// TODO: CREDITAR VALOR A JOGADOR.
		return
	}
	// Now that everything is OK, we will start up the game
	msgP1, _ := messages.NewMessage("balance_update", float64(newBalance1)/100)
	msgP2, _ := messages.NewMessage("balance_update", float64(newBalance2)/100)

	// then notify player and store it in redis.
	redisClient.UpdatePlayer(player)
	redisClient.UpdatePlayer(player2)

	redisClient.PublishPlayerEvent(player, string(msgP1))
	redisClient.PublishPlayerEvent(player2, string(msgP2))

	// Then we start a match
	roomdata, err := json.Marshal(proom)
	err = redisClient.RPushGeneric("create_game", roomdata)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleReadyQueue Creating Game RPushGeneric:%s\n", pid, err)
	}
}

func handleUnReadyQueue(player *models.Player) {
	log.Printf("[RoomWorker-%d] - Handling player (UN-READY QUEUE): %s (Session: %s, Currency: %s)\n",
		pid, player.ID, player.SessionID, player.Currency)

	proom, err := redisClient.GetRoomByID(player.RoomID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleUnReadyQueue getting player room:%s\n", pid, err)
		return
	}

	player2ID, err := proom.GetOpponentPlayerID(player.ID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleUnReadyQueue getting player opponent ID:%s\n", pid, err)
		return
	}

	player2, err := redisClient.GetPlayer(player2ID)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleUnReadyQueue getting opponent player:%s\n", pid, err)
		return
	}

	// We will always notify the opponent the we are no longer ready.
	msg, err := messages.GenerateOpponentReadyMessage(false)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handleUnReadyQueue getting GenerateOpponentReadyMessage(false) for opponent:%s\n", pid, err)
		return
	}
	redisClient.PublishPlayerEvent(player2, string(msg))

	// now we tell our player that is ready if the opponent is ready or not.
	if player2.Status != models.StatusInRoomReady {
		log.Printf("[RoomWorker-%d] - handleUnReadyQueue Opponent aint ready yet!:%s\n", pid, err)
		msg, err := messages.GenerateOpponentReadyMessage(false)
		if err != nil {
			log.Printf("[RoomWorker-%d] - Error handleUnReadyQueue getting GenerateOpponentReadyMessage(false) for player:%s\n", pid, err)
		}
		redisClient.PublishPlayerEvent(player, string(msg))
		return
	}
}

func handleJoinRoom(player *models.Player) {
	log.Printf("[RoomWorker-%d] - Handling player (JOIN ROOM): %s (Session: %s, Currency: %s)\n",
		pid, player.ID, player.SessionID, player.Currency)
	rooms, err := redisClient.GetEmptyRoomsByBetValue(player.SelectedBet)
	if err != nil {
		return
	}
	colorp1 := rand.Intn(2)
	colorp2 := 1
	if colorp1 == 1 {
		colorp2 = 0
	}
	message, err := messages.GeneratePairedMessage(rooms[0].Player1, player, rooms[0].ID, colorp1)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Error handling paired message for p1: %s\n", pid, err)
		return
	}
	message2, err2 := messages.GeneratePairedMessage(player, rooms[0].Player1, rooms[0].ID, colorp2)
	if err2 != nil {
		log.Printf("[RoomWorker-%d] - Error handling paired message for p2:%s\n", pid, err2)
		return
	}
	log.Printf("[RoomWorker-%d] - Handling player (JOIN ROOM) - Message for player1: %s\n", pid, message)
	log.Printf("[RoomWorker-%d] - Handling player (JOIN ROOM) - Message for player2: %s\n", pid, message2)

	redisClient.PublishPlayerEvent(rooms[0].Player1, string(message))
	redisClient.PublishPlayerEvent(player, string(message2))
}

func handleCreateRoom(player *models.Player) {
	log.Printf("[RoomWorker-%d] - Handling player (CREATE ROOM): %s (Session: %s, Currency: %s)\n",
		pid, player.ID, player.SessionID, player.Currency)

	room := &models.Room{
		ID:        models.GenerateUUID(),
		Player1:   player,
		StartDate: time.Now(),
		Currency:  player.Currency,
		BetValue:  player.SelectedBet,
	}
	err := redisClient.AddRoom(room)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Failed to add room to Redis: %v\n", pid, err)
		return
	}

	player.RoomID = room.ID
	player.Status = "waiting_oponente"
	redisClient.UpdatePlayer(player) // This should update out player room info.

	messageBytes, err := messages.GenerateRoomCreatedMessage(*room)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Invalid message format: %v\n", pid, err)
		return
	}
	// Publish the validated message to Redis
	err = redisClient.PublishToPlayer(*player, string(messageBytes))
	if err != nil {
		log.Printf("[RoomWorker-%d] - Failed to publish message to player: %v\n", pid, err)
		return
	}
	log.Printf("[RoomWorker-%d] - Player successfully handled and notified, %+v\n", pid, string(messageBytes))
}

func addPlayerToQueue(player *models.Player) {
	// Reset both player data.
	player.RoomID = ""
	player.GameID = ""
	player.Status = models.StatusInQueue
	redisClient.UpdatePlayer(player)

	// Pushing the player to the "queue" Redis list
	queueName := fmt.Sprintf("queue:%f", player.SelectedBet)
	err := redisClient.RPush(queueName, player)
	if err != nil {
		log.Printf("[RoomWorker-%d] - Placing player back on queue:%v\n", pid, err)
		return
	}
	redisClient.IncrementQueueCount(player.SelectedBet)
	queueMsg, _ := messages.GenerateQueueConfirmationMessage(true)
	redisClient.PublishPlayerEvent(player, string(queueMsg))
}
