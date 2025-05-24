package messages

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Lavizord/checkers-server/models"
	"github.com/Lavizord/checkers-server/redisdb"
)

type Message[T any] struct {
	Command string `json:"command"`
	Value   T      `json:"value,omitempty"`
}
type MessageSimple struct {
	Command string `json:"command"`
}

type OpponentReady struct {
	IsReady bool `json:"is_ready"`
}

type GameConnectedMessage struct {
	PlayerID   string  `json:"player_id"`
	PlayerName string  `json:"player_name"`
	Money      float64 `json:"money"`
	Status     string  `json:"status"`
}

// This one missed the json code, the FE is already working wth this... dont CHANGE the ones that dont have it.
type GameStartMessage struct {
	GameID          string `json:"game_id"`
	Board           map[string]*models.Piece
	MaxTimer        int `json:"max_timer"`
	CurrentPlayerID string
	GamePlayers     []GamePlayerResponse
	WinFactor       float64 `json:"win_factor"`
}

type GameUpdatetMessage struct {
	Board           map[string]*models.Piece
	CurrentPlayerID string `json:"current_player_id"`
	CurrentTurn     int    `json:"current_turn"`
}

type GameTimer struct {
	PlayerTimer     int    `json:"player_timer"`
	CurrentPlayerID string `json:"current_player_id"`
}

type GameOver struct {
	Reason   string             `json:"reason"`
	Winner   GamePlayerResponse `json:"winner"`
	Turns    int                `json:"turns"`
	Winnings float64            `json:"winnings"`
	GameTime time.Duration      `json:"game_time"`
}

type GenericMessage struct {
	MessageType string `json:"message_type"`
	Message     string `json:"message"`
}

func EncodeMessage[T any](command string, value T) ([]byte, error) {
	msg := Message[T]{Command: command, Value: value}
	return json.Marshal(msg)
}

func DecodeRawMessage(data []byte) (*Message[json.RawMessage], error) {
	var msg Message[json.RawMessage]
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("[Message Parser - DecodeRawMessage] invalid message format: %w", err)
	}
	return &msg, nil
}

// Decode a Fully Typed Message
func DecodeTypedMessage[T any](data []byte) (*Message[T], error) {
	var msg Message[T]
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("[Message Parser - DecodeTypedMessage] invalid message format: %w", err)
	}
	return &msg, nil
}

func NewMessage[T any](command string, value T) ([]byte, error) {
	if _, ok := validCommands[command]; !ok {
		return nil, fmt.Errorf("[Message Parser - New Message] invalid command: %s", command)
	}
	message := map[string]interface{}{
		"command": command,
		"value":   value, // Will always be included
	}
	return json.Marshal(message)
}

func GenerateGenericMessage(msgtype string, msg string) ([]byte, error) {
	genericMsg := GenericMessage{
		MessageType: msgtype,
		Message:     msg,
	}
	return NewMessage("message", genericMsg)
}

func ParseMessage(msgBytes []byte) (*Message[json.RawMessage], error) {
	msg, err := DecodeRawMessage(msgBytes)
	if err != nil {
		return nil, err
	}
	// Check if the command is in our map
	if _, ok := validCommands[msg.Command]; !ok {
		return nil, fmt.Errorf("[Message Parser] invalid command: %s", msg.Command)
	}

	// This switch is just to make sure we propperly serialize our value.
	switch msg.Command {
	case "create_room", "join_room":
		var value float64
		if err := json.Unmarshal(msg.Value, &value); err != nil {
			return nil, fmt.Errorf("[Message Parser] invalid value format for %s: %w", msg.Command, err)
		}

	case "ready_room":
		var value bool
		if err := json.Unmarshal(msg.Value, &value); err != nil {
			return nil, fmt.Errorf("[Message Parser] invalid value format for %s: %w", msg.Command, err)
		}
		return msg, nil

	case "game_info":
		var queueNumbersResponse models.QueueNumbersResponse
		if err := json.Unmarshal(msg.Value, &queueNumbersResponse); err != nil {
			return nil, fmt.Errorf("invalid value format for game_info: %w", err)
		}
		//log.Printf("[Message Parser] Parsed game_info: %+v\n", queueNumbersResponse)
	}

	return msg, nil
}

func GenerateConnectedMessage(player models.Player, balance int64) ([]byte, error) {
	connectInfo := GameConnectedMessage{
		PlayerID:   player.ID,
		PlayerName: player.Name,
		Money:      (float64(balance) / 100.0),
		Status:     string(player.Status),
	}
	return NewMessage("connected", connectInfo)
}

func GeneratePairedMessage(player1, player2 *models.Player, roomID string, color int, winnings int64, timer int) ([]byte, error) {
	pairedValue := models.PairedValue{
		Color:         color,
		Opponent:      player2.Name,
		RoomID:        roomID,
		Winnings:      float64(winnings) / 100,
		Timer:         timer,
		PlayerReady:   player1.Status == models.StatusInRoomReady,
		OpponentReady: player2.Status == models.StatusInRoomReady,
	}
	return NewMessage("paired", pairedValue)
}

func GeneratePairedMessageFromP2Name(p2Name, roomID string, playerReady, opponentReady bool, color int, winnings int64, timer int) ([]byte, error) {
	pairedValue := models.PairedValue{
		Color:         color,
		Opponent:      p2Name,
		RoomID:        roomID,
		Winnings:      float64(winnings) / 100,
		Timer:         timer,
		PlayerReady:   playerReady,
		OpponentReady: opponentReady,
	}
	return NewMessage("paired", pairedValue)
}

func GenerateRoomCreatedMessage(room models.Room) ([]byte, error) {
	roomValue := models.RoomValue{
		ID:       room.ID,
		Player:   room.Player1.Name,
		Currency: room.Currency,
		BetValue: room.BetValue,
	}
	return NewMessage("room_created", roomValue)
}

func GenerateOpponentReadyMessage(isReady bool) ([]byte, error) {
	opponentReady := OpponentReady{IsReady: isReady}
	return NewMessage("opponent_ready", opponentReady)
}

func GenerateQueueConfirmationMessage(value bool) ([]byte, error) {
	return NewMessage("queue_confirmation", value)
}

//func GenerateGameStartMessage(game models.Game) ([]byte, error) {
//	gamestart := GameStartMessage{
//		Board:           game.Board.Grid,
//		MaxTimer:        game.Players[0].Timer,
//		CurrentPlayerID: game.CurrentPlayerID,
//		GamePlayers:     game.Players,
//	}
//	return NewMessage("game_start", gamestart)
//}

type GamePlayerResponse struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Timer     int    `json:"timer"`
	Color     string `json:"color"`
	NumPieces int    `json:"num_pieces"`
}

func ConvertGamePlayerToResponse(player models.GamePlayer) GamePlayerResponse {
	return GamePlayerResponse{
		ID:        player.ID,
		Name:      player.Name,
		Timer:     player.Timer,
		Color:     player.Color,
		NumPieces: player.NumPieces,
	}
}

// For a slice of players (now can reuse the single version)
func ConvertGamePlayersToResponse(players []models.GamePlayer) []GamePlayerResponse {
	result := make([]GamePlayerResponse, len(players))
	for i, p := range players {
		result[i] = ConvertGamePlayerToResponse(p) // Reusing the single version
	}
	return result
}

func GenerateGameStartMessage(game models.Game) ([]byte, error) {
	maxTimer, _ := game.CalcGameMaxTimer()
	gamestart := GameStartMessage{
		GameID:          game.ID,
		Board:           game.Board.Grid,
		MaxTimer:        maxTimer,
		CurrentPlayerID: game.CurrentPlayerID,
		GamePlayers:     ConvertGamePlayersToResponse(game.Players),
		WinFactor:       game.OperatorIdentifier.WinFactor,
	}
	return NewMessage("game_start", gamestart)
}

func GenerateGameBoardState(game models.Game) ([]byte, error) {
	maxTimer, _ := game.CalcGameMaxTimer()
	gamestart := GameStartMessage{
		GameID:          game.ID,
		Board:           game.Board.Grid,
		MaxTimer:        maxTimer,
		CurrentPlayerID: game.CurrentPlayerID,
		GamePlayers:     ConvertGamePlayersToResponse(game.Players),
		WinFactor:       game.OperatorIdentifier.WinFactor,
	}
	return NewMessage("board_state", gamestart)
}

func GenerateGameReconnectMessage(game models.Game) ([]byte, error) {
	maxTimer, _ := game.CalcGameMaxTimer()

	gamestart := GameStartMessage{
		GameID:          game.ID,
		Board:           game.Board.Grid,
		MaxTimer:        maxTimer,
		CurrentPlayerID: game.CurrentPlayerID,
		GamePlayers:     ConvertGamePlayersToResponse(game.Players),
		WinFactor:       game.OperatorIdentifier.WinFactor,
	}
	return NewMessage("game_reconnect", gamestart)
}

func GenerateGameTimerMessage(game models.Game, timer int) ([]byte, error) {
	gamestart := GameTimer{
		PlayerTimer:     timer,
		CurrentPlayerID: game.CurrentPlayerID,
	}
	return NewMessage("game_timer", gamestart)
}

func GenerateGameOverMessage(reason string, game models.Game, winnings int64) ([]byte, error) {
	winner, err := game.GetGamePlayer(game.Winner)
	if err != nil {
		log.Printf("Error retrieving game winner player: %v\n", err)
	}

	gameover := GameOver{
		Reason:   reason,
		Winner:   ConvertGamePlayerToResponse(*winner),
		Turns:    game.Turn,
		GameTime: game.EndTime.Sub(game.StartTime),
		Winnings: float64(winnings) / 100.0,
	}
	return NewMessage("game_over", gameover)
}

func GenerateMoveMessage(move models.Move) ([]byte, error) {
	return NewMessage("move_piece", move)
}

// Helper function to marshal a value and ignore errors
func MustMarshal(v interface{}) json.RawMessage {
	bytes, _ := json.Marshal(v)
	return bytes
}

func GenerateGameInfoMessageBytes(redisClient *redisdb.RedisClient) ([]byte, error) {
	aggregates, err := redisClient.GetQueueNumberResponse()
	if err != nil {
		log.Printf("[GenerateGameInfoMessageBytes] - Error getting QueueNumber: %v\n", err)
		return nil, err
	}
	// Create a message with the game_info
	messageBytes, err := NewMessage("game_info", aggregates)
	if err != nil {
		log.Printf("[GenerateGameInfoMessageBytes] - Error creating message: %v\n", err)
		return nil, err
	}

	return messageBytes, nil
}
