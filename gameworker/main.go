package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Lavizord/checkers-server/config"
	"github.com/Lavizord/checkers-server/interfaces"
	"github.com/Lavizord/checkers-server/messages"
	"github.com/Lavizord/checkers-server/models"
	"github.com/Lavizord/checkers-server/postgrescli"
	"github.com/Lavizord/checkers-server/redisdb"
)

var pid int
var redisClient *redisdb.RedisClient
var postgresClient *postgrescli.PostgresCli
var name = "GameWorker"

func init() {
	pid = os.Getpid()
	config.LoadConfig()
	redisConData := config.Cfg.Redis
	client, err := redisdb.NewRedisClient(redisConData.Addr, redisConData.User, redisConData.Password)
	if err != nil {
		log.Fatalf("[%s-Redis] Error initializing Redis client: %v\n", name, err)
	}
	redisClient = client

	sqlcliente, err := postgrescli.NewPostgresCli(
		config.Cfg.Postgres.User,
		config.Cfg.Postgres.Password,
		config.Cfg.Postgres.DBName,
		config.Cfg.Postgres.Host,
		config.Cfg.Postgres.Port,
		config.Cfg.Postgres.Ssl,
	)
	if err != nil {
		log.Fatalf("[%s-PostgreSQL] Error initializing POSTGRES client: %v\n", name, err)
	}
	postgresClient = sqlcliente
}

func main() {
	defer func() {
		if redisClient != nil {
			redisClient.CloseRedisClient()
		}
	}()

	log.Printf("[%s-%d] - Waiting for Game messages...\n", name, pid)
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
			log.Printf("[%s-%d] - (Process Game Creation) - Error retrieving room data: %v\n", name, pid, err)
			continue
		}

		if len(roomData) < 2 {
			log.Printf("[%s-%d] - (Process Game Creation) - Unexpected BLPop result: %+v\n", name, pid, roomData)
			continue
		}
		//log.Printf("[%s-%d] - (Process Game Creation) - create game!: %+v\n", name, pid, roomData)

		var room models.Room
		err = json.Unmarshal([]byte(roomData[1]), &room) // Extract second element
		if err != nil {
			log.Printf("[%s-%d] - (Process Game Creation) - JSON Unmarshal Error: %v\n", name, pid, err)
			continue
		}

		player1, err := redisClient.GetPlayer(room.Player1.ID)
		if err != nil {
			player1 = redisClient.GetDisconnectedInQueuePlayerData(room.Player1.ID)
		}
		player2, err := redisClient.GetPlayer(room.Player2.ID)
		if err != nil {
			player2 = redisClient.GetDisconnectedInQueuePlayerData(room.Player2.ID)
		}
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

		err = redisClient.AddGame(game)
		redisClient.RemoveRoom(redisdb.GenerateRoomRedisKeyById(room.ID))
		msg, err := messages.GenerateGameStartMessage(*game)
		//log.Printf("[%s-%d] - (Process Game Creation) - Message to publish: %v\n", name, pid, string(msg))
		BroadCastToGamePlayers(msg, *game)
		// Finnally save stuff to redis.
		err = redisClient.UpdatePlayer(player1)
		if err != nil {
			// since the player is offline, we will move the player over to the offline list.
			redisClient.SaveDisconnectSessionPlayerData(*player1, *game)
			redisClient.DeleteDisconnectedInQueuePlayerData(player2.ID)
			// we also notify the opponent that this player is offline.
			msg, _ := messages.NewMessage("opponent_disconnected_game", "disconnected")
			opponent, _ := game.GetGamePlayer(player2.ID)
			redisClient.PublishToGamePlayer(*opponent, string(msg))
		}
		err = redisClient.UpdatePlayer(player2)
		if err != nil {
			// since the player is offline, we will move the player over to the offline list.
			redisClient.SaveDisconnectSessionPlayerData(*player2, *game)
			redisClient.DeleteDisconnectedInQueuePlayerData(player1.ID)
			// we also notify the opponent that this player is offline.
			msg, _ := messages.NewMessage("opponent_disconnected_game", "disconnected")
			opponent, _ := game.GetGamePlayer(player1.ID)
			redisClient.PublishToGamePlayer(*opponent, string(msg))
		}

		go startTimer(game) // Start turn timer
	}
}

func processGameMoves() {
	for {
		moveData, err := redisClient.BLPopGeneric("move_piece", 0) // Block
		if err != nil {
			log.Printf("[%s-%d] - (Process Game Moves) - Error retrieving move data: %v\n", name, pid, err)
			continue
		}
		//log.Printf("[%s-%d] - (Process Game Moves) - processing move DATA!: %+v\n", name, pid, moveData)

		// We start by getting our move data, player, game and opponentPlayer.
		var move models.Move
		err = json.Unmarshal([]byte(moveData[1]), &move) // Extract second element
		if err != nil {
			log.Printf("[%s-%d] - (Process Game Moves) - JSON Unmarshal Error: %v\n", name, pid, err)
			continue
		}
		player, err := redisClient.GetPlayer(move.PlayerID)
		if err != nil {
			log.Printf("[%s-%d] - (Process Game Moves) - Failed to get player!: %v\n", name, pid, err)
			continue
		}
		game, err := redisClient.GetGame(player.GameID)
		if err != nil {
			log.Printf("[%s-%d] - (Process Game Moves) - Failed to get game!: %v\n", name, pid, err)
			continue
		}
		if game.CurrentPlayerID != move.PlayerID {
			log.Printf("[%s-%d] - (Process Game Moves) - Incorrect current player to process move!: %v\n", name, pid, moveData)
			continue
		}

		piece := game.Board.GetPieceByID(move.PieceID)
		if !validMove(game, move, piece) {
			log.Printf("Invalid move detected")
			boardState, _ := messages.GenerateGameBoardState(*game)
			msginv, _ := messages.NewMessage("invalid_move", boardState)
			redisClient.PublishToPlayer(*player, string(msginv))
			continue
		}
		// We move our piece.
		if !game.MovePiece(move) {
			log.Printf("[%s-%d] - (Process Game Moves) - Invalid Move!: %v\n", name, pid, moveData)
			msginv, _ := messages.NewMessage("invalid_move", fmt.Sprintf("(Process Game Moves) - Invalid Move!: %v", moveData))
			redisClient.PublishToPlayer(*player, string(msginv))
			continue
		}
		game.UpdatePlayerPieces()
		move.IsKinged = game.Board.WasPieceKinged(move.To, *piece)
		if move.IsKinged {
			piece.IsKinged = move.IsKinged
		}

		// We send the message to the opponent player.
		msg, err := messages.GenerateMoveMessage(move)
		if err != nil {
			log.Printf("[%s-%d] - (Process Game Moves) - Failed to generate message: %v\n", name, pid, string(msg))
		}
		//log.Printf("[%s-%d] - (Process Game Moves) - Message to publish: %v\n", name, pid, string(msg))
		opponent, _ := game.GetOpponentGamePlayer(move.PlayerID)
		redisClient.PublishToGamePlayer(*opponent, string(msg))

		// Since the move was validated and passed to the other player, its time to check for our end turn / end game conditions.
		// This means we can add the move to our game.
		game.Moves = append(game.Moves, move)

		// We check for game Over
		if game.CheckGameOver() {
			handleGameEnd(game, "winner", move.PlayerID)
			continue
		}
		// We check for a capture.
		if !move.IsCapture {
			handleTurnChange(game)
			continue
		}
		if move.IsCapture && !game.Board.CanPieceCaptureNEW(move.To) {
			handleTurnChange(game)
			continue
		}
		if move.IsKinged {
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
			log.Printf("[%s-%d] - (Process Leave Game) - Error retrieving player data from leave game queue: %v\n", name, pid, err)
			continue
		}
		//log.Printf("[%s-%d] - Processing the leave game: %+v\n", name, pid, playerData)
		playerData, err = redisClient.GetPlayer(playerData.ID)
		if err != nil {
			log.Printf("[%s-%d] - (Process Leave Game) - Error re-fetching player data.: %v\n", name, pid, err)
			continue
		}
		game, err := redisClient.GetGame(playerData.GameID)
		if err != nil {
			log.Printf("[%s-%d] - Error retrieving Game:%v\n", name, pid, err)
			continue
		}
		winnrID, _ := game.GetOpponentPlayerID(playerData.ID)
		handleGameEnd(game, "player_left", winnrID)
	}
}

func processDisconnectFromGame() {
	for {
		// Block until there is a game over message
		// TODO: Update player date, its without a game.
		playerData, err := redisClient.BLPop("disconnect_game", 0)
		if err != nil {
			log.Printf("[%s-%d] - (Process Disconnect Game) - Error retrieving player data from queue: %v\n", name, pid, err)
			continue
		}
		//playerData, err = redisClient.GetPlayer(playerData.ID)	// We cant do the get player, because it was already removed...
		//log.Printf("[%s-%d] - Processing the disconnect from game: %+v\n", name, pid, playerData)
		game, err := redisClient.GetGame(playerData.GameID)
		if err != nil {
			log.Printf("[%s-%d] - Error retrieving Game:%v\n", name, pid, err)
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
			log.Printf("[%s-%d] - (Process Reconnect Game) - Error retrieving player data from queue: %v\n", name, pid, err)
			continue
		}
		log.Printf("[%s-%d]  (Process Reconnect Game) - Processing the reconnect game: %+v\n", name, pid, playerData)
		game, err := redisClient.GetGame(playerData.GameID)
		if err != nil {
			log.Printf("[%s-%d] - (Process Reconnect Game) - Error retrieving game data from redis: %v\n", name, pid, err)
			continue
		}
		// We send a message to the reconnected player with the board state.
		msg, err := messages.GenerateGameReconnectMessage(*game)
		if err != nil {
			log.Printf("[%s-%d] - (Process Reconnect Game) - Error generating game reconnect message: %v\n", name, pid, err)
			continue
		}
		err = redisClient.PublishToPlayer(*playerData, string(msg))
		if err != nil {
			log.Printf("[%s-%d] - (Process Reconnect Game) - Error publishing game reconnect message: %v\n", name, pid, err)
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
		log.Printf("[%s-%d] - (Handle Turn Change) - Failed to generate for turn change: %v\n", name, pid, msg)
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
	//activePlayer := game.Players[activePlayerIndex]	// This was used in the log prints.
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
			if isEven(timer) {
				redisClient.PublishToGamePlayer(game.Players[0], string(msg))
				redisClient.PublishToGamePlayer(game.Players[1], string(msg))
			}

			// Check if the timer has expired
			if activePlayerTimer <= 0 {
				handleTurnChange(game)
				//log.Printf("Turn timer expired for player %s in game %s. Switching turns.\n", activePlayer.ID, game.ID)
			}

		case msg := <-pubsub.Channel():
			switch msg.Channel {
			case stopChannel:
				//log.Printf("Timer stopped for game %s\n", game.ID)
				return // Exit the function, stopping the timer

			case switchChannel:
				// Switch the active player when a move is made
				activePlayerIndex = 1 - activePlayerIndex // Toggle between 0 and 1
				//activePlayer = game.Players[activePlayerIndex] 	// This was used in the log prints.
				timer = baseTimer // Reset the timer for the new active player
				//log.Printf("Switched active player to %s in game %s. Timer reset to %d seconds.\n", activePlayer.ID, game.ID, timer)
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
			game.UpdatePlayerTimer(activePlayer.ID, activePlayerTimer)
			go redisClient.UpdateGame(game)
			go redisClient.PublishToGamePlayer(game.Players[0], string(msg))
			go redisClient.PublishToGamePlayer(game.Players[1], string(msg))

			// Check if the active player's timer has expired
			if activePlayerTimer <= 0 {
				// The other player wins
				winner := game.Players[1-activePlayerIndex].ID
				//log.Printf("Cumulative timer expired for player %s in game %s. Player %s wins.\n", activePlayer.ID, game.ID, winner)
				handleGameEnd(game, "timeout", winner)
				return
			}

		case msg := <-pubsub.Channel():
			switch msg.Channel {
			case stopChannel:
				//log.Printf("Timer stopped for game %s\n", game.ID)
				return // Exit the function, stopping the timer

			case switchChannel:
				// Switch the active player when a switch is sent
				activePlayerIndex = 1 - activePlayerIndex // Toggle between 0 and 1
				activePlayer = game.Players[activePlayerIndex]
				//log.Printf("Switched active player to %s in game %s\n", activePlayer.ID, game.ID)
			}
		}
	}
}

func handleGameEnd(game *models.Game, reason string, winnerID string) {
	//log.Printf("Handling Game End for game [%v] - reason: [%v]", game.ID, reason)

	publishStopToTimerChannel(game.ID)
	game.FinishGame(winnerID)

	winAmount := interfaces.CalculateWinAmount(int64(game.BetValue*100), game.OperatorIdentifier.WinFactor)
	gameOverMsg, err := messages.GenerateGameOverMessage(reason, *game, winAmount)
	if err != nil {
		log.Printf("[%s] - (Handle Game Over) - Failed to generate game over message!: %v\n", name, err)
		return
	}
	p1 := game.Players[0]
	p2 := game.Players[1]
	go cleanUpGameDisconnectedPlayers(*game) // This was at the end, was moved up here, might make the reconect when the game is over more smooth...?
	go processGameEndForPlayer(winnerID, game, p1, reason, winAmount, gameOverMsg)
	go processGameEndForPlayer(winnerID, game, p2, reason, winAmount, gameOverMsg)

	// since the game is Over, we remove it from redis.
	if redisClient.RemoveGame(game.ID) != nil {
		log.Printf("[%s] - (Handle Game Over) - Failed to remove game!: %v\n", name, err)
	} else {
		log.Printf("[%s] - (Handle Game Over) - Removed game!: %v\n", name, err)
	}
	postgresClient.SaveGame(*game, reason)
}

func processGameEndForPlayer(winnerID string, game *models.Game, gamePlayer models.GamePlayer, reason string, winAmount int64, gameOverMsg []byte) {
	var interfaceModule interfaces.OperatorInterface
	interfaceModule = interfaces.OperatorModules[game.OperatorIdentifier.OperatorName]
	var balanceUpdateMsg []byte

	// 1. The Winner needs to have a post to the wallet.
	if winnerID == gamePlayer.ID {
		// Get the session from the ID, since they share the same ID.
		winnerSession, err := redisClient.GetSessionByID(winnerID)
		if err != nil {
			log.Printf("[GameWorker] - error -> handleGameEnd - processGameEndForPlayer: fetching winner player session:%s\n", err)
			return
		}
		if winnerSession == nil {
			log.Printf("[GameWorker] - error -> handleGameEnd - processGameEndForPlayer: session id is nill!:%s\n", err)
			return
		}
		// we use our winner session here, because this way the winner will be payed out even if offline.
		var newBalance int64
		newBalance, _, err = interfaceModule.HandlePostWin(postgresClient, redisClient, *winnerSession, int64(game.BetValue*100), game.ID)
		if err != nil {
			log.Printf("[RoomWorker] - Error posting the win :%s\n", err)
		} else {
			// We then generate the balance update message and send it over to the game Player. The player can be offline, but I guess the message just wont get delivered.
			// I could try to fetch the player from redis...? Is it worth it?... I fetch the player down the line...
			balanceUpdateMsg, _ = messages.NewMessage("balance_update", float64(newBalance)/100)
		}
	}

	// 2. Update player data, if it exists. If not prolly offline.
	player, err := redisClient.GetPlayer(gamePlayer.ID)
	if err != nil {
		log.Printf("[%s-%d] - (Handle Game Over) - processGameEndForPlayer - Failed to get player!: %v\n", name, pid, err)
		return
	}
	if player == nil {
		log.Printf("[%s-%d] - (Handle Game Over) - processGameEndForPlayer - player is nill !: %v\n", name, pid, err)
		return
	}
	player.GameID = ""
	player.UpdatePlayerStatus(models.StatusOnline)
	redisClient.UpdatePlayer(player)
	redisClient.PublishToGamePlayer(gamePlayer, string(gameOverMsg))
	redisClient.PublishToGamePlayer(gamePlayer, string(balanceUpdateMsg))

}

func cleanUpGameDisconnectedPlayers(game models.Game) {
	p1SessionId := game.Players[0].SessionID
	p2SessionId := game.Players[1].SessionID

	discPlayer1 := redisClient.GetDisconnectedPlayerData(p1SessionId)
	if discPlayer1 != nil {
		redisClient.DeleteDisconnectedPlayerSession(p1SessionId)
		redisClient.RemovePlayer(discPlayer1.ID)
	}
	discPlayer2 := redisClient.GetDisconnectedPlayerData(p2SessionId)
	if discPlayer2 != nil {
		redisClient.DeleteDisconnectedPlayerSession(p2SessionId)
		redisClient.RemovePlayer(discPlayer2.ID)
	}

}

func validMove(game *models.Game, move models.Move, piece *models.Piece) bool {
	capturers := game.Board.PiecesThatCanCapture(game.CurrentPlayerID)
	// If captures are available, and this piece can't capture, reject the move
	if len(capturers) > 0 {
		canCapture := false
		for _, p := range capturers {
			if p.PieceID == piece.PieceID {
				canCapture = true
				break
			}
		}
		if !canCapture {
			log.Println("Error: there are player pieces that can capture, must move one of those.")
			return false
		}
	}

	var valid bool
	var err error
	game.Board.PiecesThatCanCapture(game.CurrentPlayerID)
	if piece.IsKinged {
		valid, err = game.Board.IsValidMoveKing(move)
	} else {
		valid, err = game.Board.IsValidMove(move)
	}
	if err != nil {
		log.Printf("Error: %v", err)
		return false
	}
	if !valid {
		return false
	}
	return true
}

func isEven(n int) bool {
	return n&1 == 0 // Last bit = 0 → even
}

func BroadCastToGamePlayers(msg []byte, game models.Game) {
	redisClient.PublishToGamePlayer(game.Players[0], string(msg))
	redisClient.PublishToGamePlayer(game.Players[1], string(msg))
}
