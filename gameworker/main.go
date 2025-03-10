package main

import (
	"checkers-server/config"
	"checkers-server/messages"
	"checkers-server/models"
	"checkers-server/redisdb"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

var pid int
var redisClient *redisdb.RedisClient
var name = "GameWorker"

func init() {
	pid = os.Getpid()
	config.LoadConfig()
	redisAddr := config.Cfg.Redis.Addr
	client, err := redisdb.NewRedisClient(redisAddr)
	if err != nil {
		log.Fatalf("[%s-Redis] Error initializing Redis client: %v\n", name, err)
	}
	redisClient = client
}

func main() {
	fmt.Printf("[%s-%d] - Waiting for Game messages...\n", name, pid)
	go processGameCreation()
	go processGameMoves()
	go processLeaveGame()
	go processDisconnectFromGame()
	go processReconnectFromGame()
	select {}
}

func processGameCreation() {
	for {
		roomData, err := redisClient.BLPopGeneric("create_game", 0) // Block
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Game Creation) - Error retrieving room data: %v\n", name, pid, err)
			continue
		}

		if len(roomData) < 2 {
			fmt.Printf("[%s-%d] - (Process Game Creation) - Unexpected BLPop result: %+v\n", name, pid, roomData)
			continue
		}
		fmt.Printf("[%s-%d] - (Process Game Creation) - create game!: %+v\n", name, pid, roomData)

		var room models.Room
		err = json.Unmarshal([]byte(roomData[1]), &room) // Extract second element
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Game Creation) - JSON Unmarshal Error: %v\n", name, pid, err)
			continue
		}

		player1, err := redisClient.GetPlayer(room.Player1.ID)
		player2, err := redisClient.GetPlayer(room.Player2.ID)
		game := room.NewGame()
		// we need to update our players with a game ID.
		player1.GameID = game.ID
		player2.GameID = game.ID
		// We then reset the room id
		player1.RoomID = ""
		player2.RoomID = ""
		// we also change the player status.
		player1.UpdatePlayerStatus(models.StatusInGame)
		player2.UpdatePlayerStatus(models.StatusInGame)
		// Finnally save stuff to redis.
		err = redisClient.UpdatePlayer(player1)
		err = redisClient.UpdatePlayer(player2)
		err = redisClient.AddGame(game)
		redisClient.RemoveRoom(redisdb.GenerateRoomRedisKeyById(room.ID))
		msg, err := messages.GenerateGameStartMessage(*game)

		fmt.Printf("[%s-%d] - (Process Game Creation) - Message to publish: %v\n", name, pid, string(msg))
		BroadCastToGamePlayers(msg, *game)
		go startTimer(game) // Start turn timer
	}
}

func processGameMoves() {
	for {
		moveData, err := redisClient.BLPopGeneric("move_piece", 0) // Block
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Game Moves) - Error retrieving move data: %v\n", name, pid, err)
			continue
		}
		fmt.Printf("[%s-%d] - (Process Game Moves) - processing move DATA!: %+v\n", name, pid, moveData)

		// We start by getting our move data, player, game and opponentPlayer.
		var move models.Move
		err = json.Unmarshal([]byte(moveData[1]), &move) // Extract second element
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Game Moves) - JSON Unmarshal Error: %v\n", name, pid, err)
			continue
		}
		player, err := redisClient.GetPlayer(move.PlayerID)
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Game Moves) - Failed to get player!: %v\n", name, pid, err)
			continue
		}
		game, err := redisClient.GetGame(player.GameID)
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Game Moves) - Failed to get game!: %v\n", name, pid, err)
			continue
		}
		opponent, err := game.GetOpponentGamePlayer(move.PlayerID)

		// We move our piece.
		game.MovePiece(move)
		game.UpdatePlayerPieces()
		// We send the message to the opponent player.
		msg, err := messages.GenerateMoveMessage(move)
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Game Moves) - Failed to generate message: %v\n", name, pid, string(msg))
		}
		fmt.Printf("[%s-%d] - (Process Game Moves) - Message to publish: %v\n", name, pid, string(msg))
		redisClient.PublishToGamePlayer(*opponent, string(msg))

		// We check for game Over
		if game.CheckGameOver() {
			handleGameEnd(*game, "winner", move.PlayerID)
			continue
		}
		// We check for a capture.
		if !move.IsCapture {
			handleTurnChange(game)
			continue
		}
		// we check for a turn change.
		if move.IsCapture && !game.Board.CanPieceCapture(move.To) {
			handleTurnChange(game)
			continue
		}
		redisClient.UpdateGame(game) // we update our game at the end.
	}
}

func processLeaveGame() {
	for {
		// Block until there is a game over message
		playerData, err := redisClient.BLPop("leave_game", 0)
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Leave Game) - Error retrieving player data from queue: %v\n", name, pid, err)
			continue
		}
		//playerData, err = redisClient.GetPlayer(playerData.ID)	// We cant do the get player, because it was already removed...
		fmt.Printf("[%s-%d] - Processing the leave game: %+v\n", name, pid, playerData)
		game, err := redisClient.GetGame(playerData.GameID)
		if err != nil {
			fmt.Printf("[%s-%d] - Error retrieving Game:%v\n", name, pid, err)
			continue
		}
		winnrID, _ := game.GetOpponentPlayerID(playerData.ID)
		handleGameEnd(*game, "player_left", winnrID)
	}
}

func processDisconnectFromGame() {
	for {
		// Block until there is a game over message
		playerData, err := redisClient.BLPop("disconnect_game", 0)
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Disconnect Game) - Error retrieving player data from queue: %v\n", name, pid, err)
			continue
		}
		//playerData, err = redisClient.GetPlayer(playerData.ID)	// We cant do the get player, because it was already removed...
		fmt.Printf("[%s-%d] - Processing the leave game: %+v\n", name, pid, playerData)
		game, err := redisClient.GetGame(playerData.GameID)
		if err != nil {
			fmt.Printf("[%s-%d] - Error retrieving Game:%v\n", name, pid, err)
			continue
		}
		redisClient.SaveDisconnectSessionPlayerData(*playerData, *game)
		gamePlayer, _ := game.GetGamePlayer(playerData.ID)

		// Now we notify the other player that this happened
		msg, _ := messages.NewMessage("opponent_disconnected_game", "disconnected")
		opponent, _ := game.GetOpponentGamePlayer(gamePlayer.ID)
		redisClient.PublishToGamePlayer(*opponent, string(msg))

	}
}

func processReconnectFromGame() {
	for {
		// Block until there is a game over message
		playerData, err := redisClient.BLPop("reconnect_game", 0)
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Reconnect Game) - Error retrieving player data from queue: %v\n", name, pid, err)
			continue
		}
		fmt.Printf("[%s-%d]  (Process Reconnect Game) - Processing the reconnect game: %+v\n", name, pid, playerData)

		game, err := redisClient.GetGame(playerData.GameID)
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Reconnect Game) - Error retrieving game data from redis: %v\n", name, pid, err)
			continue
		}

		// We send a message to the reconnected player with the board state.
		msg, err := messages.GenerateGameReconnectMessage(*game)
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Reconnect Game) - Error generating game reconnect message: %v\n", name, pid, err)
			continue
		}
		err = redisClient.PublishToPlayer(*playerData, string(msg))
		if err != nil {
			fmt.Printf("[%s-%d] - (Process Reconnect Game) - Error publishing game reconnect message: %v\n", name, pid, err)
			continue
		}

		// We notify the opponent that the player reconnected.
		opponent, _ := game.GetOpponentGamePlayer(playerData.ID)
		msg, _ = messages.NewMessage("opponent_disconnected_game", "reconnected")
		redisClient.PublishToGamePlayer(*opponent, string(msg))

		redisClient.DeleteDisconnectedPlayerSession(playerData.SessionID)
	}
}

func handleTurnChange(game *models.Game) {
	// publishStopToTimerChannel(game.ID)
	game.NextPlayer()
	redisClient.UpdateGame(game)
	msg, err := messages.NewMessage("turn_switch", game.CurrentPlayerID)
	if err != nil {
		fmt.Printf("[%s-%d] - (Handle Turn Change) - Failed to generate for turn change: %v\n", name, pid, msg)
	}
	BroadCastToGamePlayers(msg, *game)
	publishSwitchToTimerChannel(game.ID) // Start a fresh timer or switch player timer.
}

func publishStopToTimerChannel(gameID string) {
	stopChannel := fmt.Sprintf("game:%s:stop_timer", gameID)
	redisClient.Client.Publish(context.Background(), stopChannel, "STOP") // Stop the old timer
}

func publishSwitchToTimerChannel(gameID string) {
	switchChannel := fmt.Sprintf("game:%s:switch", gameID)
	redisClient.Client.Publish(context.Background(), switchChannel, "SWITCH") // This will let the timer know there was a change.
}

func startTimer(game *models.Game) {
	switch game.TimerSetting {
	case "reset":
		startResetEveryTurnTimer(game)
	case "cumulative":
		startCumulativeTimer(game)
	default:
		log.Printf("Invalid timer setting: %s for game %s\n", game.TimerSetting, game.ID)
	}
}

func startResetEveryTurnTimer(game *models.Game) {
	ctx := context.Background()
	stopChannel := fmt.Sprintf("game:%s:stop_timer", game.ID)
	switchChannel := fmt.Sprintf("game:%s:switch", game.ID) // Channel to listen for switch events

	// Subscribe to both stop and switch channels
	pubsub := redisClient.Client.Subscribe(ctx, stopChannel, switchChannel)
	defer pubsub.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Fetch the base timer from the config.
	baseTimer := config.Cfg.Services["gameworker"].Timer

	// Find the player with Color == "b" to start the timer
	var activePlayerIndex int
	for i, player := range game.Players {
		if player.Color == "b" {
			activePlayerIndex = i
			break
		}
	}
	activePlayer := game.Players[activePlayerIndex]

	// Initialize the timer for the active player
	timer := baseTimer

	for {
		select {
		case <-ticker.C:
			// Decrement the timer
			timer--
			activePlayerTimer := timer

			// Publish the updated timer to both players
			msg, _ := messages.GenerateGameTimerMessage(*game, activePlayerTimer)
			redisClient.PublishToGamePlayer(game.Players[0], string(msg))
			redisClient.PublishToGamePlayer(game.Players[1], string(msg))

			// Check if the timer has expired
			if activePlayerTimer <= 0 {
				handleTurnChange(game)
				fmt.Printf("Turn timer expired for player %s in game %s. Switching turns.\n", activePlayer.ID, game.ID)
			}

		case msg := <-pubsub.Channel():
			switch msg.Channel {
			case stopChannel:
				fmt.Printf("Timer stopped for game %s\n", game.ID)
				return // Exit the function, stopping the timer

			case switchChannel:
				// Switch the active player when a move is made
				activePlayerIndex = 1 - activePlayerIndex // Toggle between 0 and 1
				activePlayer = game.Players[activePlayerIndex]

				// Reset the timer for the new active player
				timer = baseTimer
				fmt.Printf("Switched active player to %s in game %s. Timer reset to %d seconds.\n", activePlayer.ID, game.ID, timer)
			}
		}
	}
}

func startCumulativeTimer(game *models.Game) {
	ctx := context.Background()
	stopChannel := fmt.Sprintf("game:%s:stop_timer", game.ID)
	switchChannel := fmt.Sprintf("game:%s:switch", game.ID) // Channel to listen for move events

	// Subscribe to both stop and move channels
	pubsub := redisClient.Client.Subscribe(ctx, stopChannel, switchChannel)
	defer pubsub.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Initialize timers for each player
	playerTimers := make(map[string]int) // Key: Player ID, Value: Remaining time
	for _, player := range game.Players {
		playerTimers[player.ID] = player.Timer
	}

	// Find the player with Color == "b" to start the timer
	var activePlayerIndex int
	for i, player := range game.Players {
		if player.Color == "b" {
			activePlayerIndex = i
			break
		}
	}
	activePlayer := game.Players[activePlayerIndex]

	for {
		select {
		case <-ticker.C:
			// Decrement the active player's timer
			playerTimers[activePlayer.ID]--
			activePlayerTimer := playerTimers[activePlayer.ID]

			// Publish the updated timer to both players
			game, _ := redisClient.GetGame(game.ID)
			msg, _ := messages.GenerateGameTimerMessage(*game, activePlayerTimer)
			redisClient.PublishToGamePlayer(game.Players[0], string(msg))
			redisClient.PublishToGamePlayer(game.Players[1], string(msg))

			// Check if the active player's timer has expired
			if activePlayerTimer <= 0 {
				// The other player wins
				winner := game.Players[1-activePlayerIndex].ID
				handleGameEnd(*game, "timeout", winner)
				fmt.Printf("Cumulative timer expired for player %s in game %s. Player %s wins.\n", activePlayer.ID, game.ID, winner)
				return
			}

		case msg := <-pubsub.Channel():
			switch msg.Channel {
			case stopChannel:
				fmt.Printf("Timer stopped for game %s\n", game.ID)
				return // Exit the function, stopping the timer

			case switchChannel:
				// Switch the active player when a switch is sent
				activePlayerIndex = 1 - activePlayerIndex // Toggle between 0 and 1
				activePlayer = game.Players[activePlayerIndex]
				fmt.Printf("Switched active player to %s in game %s\n", activePlayer.ID, game.ID)
			}
		}
	}
}

func handleGameEnd(game models.Game, reason string, winnerID string) {
	// if the game is over, lets stop the timers.
	publishStopToTimerChannel(game.ID)
	game.FinishGame(winnerID)
	msg, err := messages.GenerateGameOverMessage(reason, game)
	if err != nil {
		fmt.Printf("[%s-%d] - (Handle Game Over) - Failed to get game!: %v\n", name, pid, err)
		return
	}
	redisClient.PublishToGamePlayer(*&game.Players[0], string(msg))
	redisClient.PublishToGamePlayer(*&game.Players[1], string(msg))

	// Now we update the winner player
	winnerPlayer, err := redisClient.GetPlayer(game.Winner)
	if err != nil {
		fmt.Printf("[%s-%d] - (Handle Game Over) - Failed to get winner player!: %v\n", name, pid, err)
		return
	} else {
		// Update status and game Id of players
		winnerPlayer.GameID = ""
		winnerPlayer.UpdatePlayerStatus(models.StatusOnline)
		winnerPlayer.UpdateBalance(game.BetValue * 1.75)
		msgP1, _ := messages.NewMessage("balance_update", winnerPlayer.CurrencyAmount)
		redisClient.PublishPlayerEvent(winnerPlayer, string(msgP1))
		redisClient.UpdatePlayer(winnerPlayer)
	}

	opponentID, _ := game.GetOpponentPlayerID(winnerPlayer.ID)
	opponentPlayer, err := redisClient.GetPlayer(opponentID)
	if err != nil {
		fmt.Printf("[%s-%d] - (Handle Game Over) - Failed to get looser player!: %v\n", name, pid, err)
		return
	} else {
		// Update status and game Id of players
		opponentPlayer.GameID = ""
		opponentPlayer.UpdatePlayerStatus(models.StatusOnline)
		redisClient.UpdatePlayer(opponentPlayer)
	}

	// since the game is Over, we remove it from redis.
	if redisClient.RemoveGame(game.ID) != nil {
		fmt.Printf("[%s-%d] - (Handle Game Over) - Failed to remove game!: %v\n", name, pid, err)
	} else {
		fmt.Printf("[%s-%d] - (Handle Game Over) - Removed game!: %v\n", name, pid, err)
	}
}

func BroadCastToGamePlayers(msg []byte, game models.Game) {
	redisClient.PublishToGamePlayer(game.Players[0], string(msg))
	redisClient.PublishToGamePlayer(game.Players[1], string(msg))
}
