package models

import (
	"fmt"
)

type PlayerStatus string

const (
	StatusOffline     PlayerStatus = "OFFLINE"
	StatusOnline      PlayerStatus = "ONLINE"
	StatusInQueue     PlayerStatus = "IN_QUEUE"
	StatusInRoom      PlayerStatus = "IN_ROOM"
	StatusInRoomReady PlayerStatus = "IN_ROOM_READY"
	StatusInGame      PlayerStatus = "IN_GAME"
)

type Player struct {
	ID                 string             `json:"id"`
	Token              string             `json:"token"`
	RoomID             string             `json:"room_id"`
	GameID             string             `json:"game_id"`
	SessionID          string             `json:"session_id"`
	Currency           string             `json:"currency"`
	Status             PlayerStatus       `json:"status"`
	SelectedBet        float64            `json:"selected_bet"`
	Name               string             `json:"name"`
	OperatorIdentifier OperatorIdentifier `json:"operator_identifier"`
	DisconnectedAt     int64              `json:"disconnected_at"` // Unix timestamp
}

var DamasValidBetAmounts = []float64{0.5, 1, 3, 5, 10, 25, 50, 100}

// This map will hold the valid status transition
var validStatusTransitions = map[PlayerStatus]map[PlayerStatus]bool{
	StatusOffline: {
		StatusOnline: true,
	},
	StatusOnline: {
		StatusOffline: true,
		StatusInQueue: true,
	},
	StatusInQueue: {
		StatusOnline: true,
		StatusInRoom: true,
	},
	StatusInRoom: {
		//StatusInQueue:   true,
		StatusOnline:      true,
		StatusInRoomReady: true,
	},
	StatusInRoomReady: {
		StatusOnline: true,
		StatusInRoom: true,
		StatusInGame: true,
	},
	StatusInGame: {
		StatusOnline:  true,
		StatusOffline: true,
	},
}

// This updates and checks that our player status is the right one.
func (p *Player) UpdatePlayerStatus(newStatus PlayerStatus) error {
	if p.Status == newStatus {
		return fmt.Errorf("player is already in status %s", newStatus)
	}

	if valid, ok := validStatusTransitions[PlayerStatus(p.Status)][newStatus]; !ok || !valid {
		return fmt.Errorf("invalid status transition from %s to %s", p.Status, newStatus)
	}

	p.Status = newStatus
	return nil
}

func (p *Player) SetStatusOnline() {
	p.RoomID = ""
	p.GameID = ""
	p.Status = StatusOnline
}

func (p *Player) IsEligibleForQueue(queueBet float64) bool {
	if p == nil || p.Status != StatusInQueue || p.SelectedBet != queueBet {
		return false
	}
	return true
}
