package messages

import (
	"checkers-server/models"
	"encoding/json"
	"fmt"
	"time"
)

type Message[T any] struct {
	Command string `json:"command"`
	Value   T      `json:"value,omitempty"`
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

// This one missed the json code, the FE is already working wth this... dont CHANGE
type GameStartMessage struct {
	Board           map[string]*models.Piece
	CurrentPlayerID string
	GamePlayers     []models.GamePlayer
}

type GameUpdatetMessage struct {
	Board           map[string]*models.Piece
	CurrentPlayerID string `json:"current_player_id"`
	CurrentTurn     int    `json:"current_turn"`
}

type GameTimer struct {
	PlayerTimer     int `json:"player_timer"`
	CurrentPlayerID string  `json:"current_player_id"`
}

type GameOver struct {
	Reason string			`json:"reason"`
	Winner models.GamePlayer	 `json:"winner"`
	Turns int				`json:"turns"`
	GameTime time.Duration `json:"game_time"`
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
	message := Message[T]{
		Command: command,
		Value:   value,
	}
	return json.Marshal(message)
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
		var roomAggregateResponse models.RoomAggregateResponse
		if err := json.Unmarshal(msg.Value, &roomAggregateResponse); err != nil {
			return nil, fmt.Errorf("invalid value format for game_info: %w", err)
		}
		fmt.Printf("[Message Parser] Parsed game_info: %+v\n", roomAggregateResponse)
	}

	return msg, nil
}

func GenerateConnectedMessage(player models.Player) ([]byte, error) {
	connectInfo := GameConnectedMessage{
		PlayerID:   player.ID,
		PlayerName: player.Name,
		Money:      player.CurrencyAmount,
		Status:     string(player.Status),
	}
	return NewMessage("connected", connectInfo)
}

func GeneratePairedMessage(player1, player2 *models.Player, roomID string, color int) ([]byte, error) {
	pairedValue := models.PairedValue{
		Color:    color,
		Opponent: player2.Name,
		RoomID:   roomID,
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

func GenerateGameStartMessage(game models.Game) ([]byte, error) {
	gamestart := GameStartMessage {
		Board: game.Board,
		CurrentPlayerID: game.CurrentPlayerID,
		GamePlayers: game.Players,
	}
	return NewMessage("game_start", gamestart)
}

func GenerateGameTimerMessage(game models.Game, timer int) ([]byte, error) {
	gamestart := GameTimer {
		PlayerTimer: timer,
		CurrentPlayerID: game.CurrentPlayerID,
	}
	return NewMessage("game_timer", gamestart)
}

func GenerateGameOverMessage(reason string, game models.Game) ([]byte, error) {
	winner, err := game.GetGamePlayer(game.CurrentPlayerID)
	if err	!= nil {
		fmt.Printf("Error retrieving game winner player: %v\n", err)

	}

	gameover := GameOver {
		Reason: reason,
		Winner: *winner,
		Turns: game.Turn,
		GameTime: game.EndTime.Sub(game.StartTime), 		
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
