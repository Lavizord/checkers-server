package models

import (
	"log"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
)

type GameLaunchResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type Session struct {
	ID                 string             `json:"session_id"`
	Token              string             `json:"token"`
	PlayerName         string             `json:"player_name"`
	Currency           string             `json:"currency"`
	OperatorBaseUrl    string             `json:"operator_base_url"`
	CreatedAt          time.Time          `json:"created_at"`
	ExtractID          int64              `json:"extract_id"` // This was created to store the extract ID of a bet, so that we can later use it in the win post...
	OperatorIdentifier OperatorIdentifier `json:"operator_identifier"`
}

func (s *Session) IsTokenExpired() bool {
	token, _, err := new(jwt.Parser).ParseUnverified(s.Token, jwt.MapClaims{})
	if err != nil {
		log.Println("Error parsing token:", err)
		return true // Assume expired if we can't parse
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		log.Println("Invalid token claims")
		return true
	}

	// Extract expiration time (exp)
	expFloat, ok := claims["exp"].(float64)
	if !ok {
		log.Println("Expiration claim missing")
		return true
	}

	expTime := time.Unix(int64(expFloat), 0)
	return time.Now().After(expTime)
}

type Transaction struct {
	ID          string    `json:"transaction_id"` // Unique ID for each transaction
	SessionID   string    `json:"session_id"`     // Session ID for the player
	Type        string    `json:"type"`           // Type of transaction: 'bet' or 'win'
	Amount      int64     `json:"amount"`         // Amount in cents
	Currency    string    `json:"currency"`       // Currency code (e.g., EUR, USD)
	Platform    string    `json:"platform"`       // Platform name
	Operator    string    `json:"operator"`       // Operator name (e.g., SokkerDuel)
	Client      string    `json:"client"`         // Client ID (player ID)
	Game        string    `json:"game"`           // Internal game name
	Status      string    `json:"status"`         // HTTP status code
	Description string    `json:"description"`    // Description (e.g., "Insufficient Funds" or "OK")
	RoundID     string    `json:"round_id"`       // Foreign key to the round / game
	Timestamp   time.Time `json:"timestamp"`      // Timestamp in UTC
}

type OperatorIdentifier struct {
	OperatorName     string  `json:"operator_name"`
	OperatorGameName string  `json:"operator_game_name"`
	GameName         string  `json:"game_name"`
	WinFactor        float64 `json:"win_factor"`
}

type PlayerCountPerBetValue struct {
	BetValue    float64 `json:"bet_value"`
	PlayerCount int64   `json:"player_count"`
}

type QueueNumbersResponse struct {
	QueuNumbers []PlayerCountPerBetValue `json:"player_count_per_bet_value"`
}

type RoomValue struct {
	ID       string  `json:"id"`
	Player   string  `json:"name"`
	Currency string  `json:"currency"`
	BetValue float64 `json:"bet_value"`
}

type CreateRoomMessage struct {
	Command string    `json:"command"`
	Value   RoomValue `json:"value"`
}

type PairedValue struct {
	Color         int     `json:"color"`
	Opponent      string  `json:"opponent"`
	RoomID        string  `json:"room_id"`
	Winnings      float64 `json:"winnings"`
	Timer         int     `json:"timer"`
	PlayerReady   bool    `json:"player_ready"`
	OpponentReady bool    `json:"opponent_ready"`
}

type QueueConfirmation struct {
	IsConfirmed bool `json:"is_confirmed"`
}

func GenerateUUID() string {
	return uuid.New().String() // Generates a new UUID and returns it as a string
}
