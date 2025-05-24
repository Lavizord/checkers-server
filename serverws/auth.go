package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/Lavizord/checkers-server/interfaces"
	"github.com/Lavizord/checkers-server/models"
	"github.com/Lavizord/checkers-server/redisdb"
)

// Method to validate that the request is valid
func AuthValid(w http.ResponseWriter, r *http.Request, redis *redisdb.RedisClient) (bool, *models.Session, error) {
	token := r.URL.Query().Get("token")
	sessionID := r.URL.Query().Get("sessionid")
	currency := r.URL.Query().Get("currency")
	log.Printf("[wsapi] - HandleConnection: token[%v], sessionid[%v], currency[%v]\n", token, sessionID, currency)

	// We will use the url params to check if the session is valid.
	session, err := fetchAndValidateSession(token, sessionID, currency, redis)
	if err != nil {
		// If our validate session method returns an erros, something is not OK with the session and we let the client know.
		http.Error(w, fmt.Sprintf("Unauthorized: token[%v], sessionid[%v], currency[%v]", token, sessionID, currency), http.StatusUnauthorized)
		// If redis returned a session that is no longer valid, we will remove it from redis.
		// This could happen when the player is connecting with an expired token for example, the session is old, so we should remove it.
		// the session is created when there is a gamelaunch, so if we have an old session we should just delete it so that the player has
		// to do another gamelaunch.
		if session != nil {
			redis.RemoveSession(session.ID)
		}
		return false, session, err
	}
	return true, session, nil
}

func fetchAndValidateSession(token, sessionID, currency string, redis *redisdb.RedisClient) (*models.Session, error) {
	session, err := redis.GetSessionByID(sessionID)
	if err != nil {
		log.Printf("[FetchAndValidateSession] - Error fetching session from Redis: %v\n", err)
		return nil, fmt.Errorf("[Session] - failed to fetch session: %v", err)
	}
	//log.Printf("[FetchAndValidateSession] - Session fetched from Redis: %+v\n", session)

	if session.Currency != currency {
		log.Printf("[FetchAndValidateSession] - Currency mismatch: expected %s, got %s\n", currency, session.Currency)
		return nil, fmt.Errorf("[Session] - currency mismatch: expected %s, got %s", currency, session.Currency)
	}
	//log.Printf("[FetchAndValidateSession] - Currency validation successful\n")

	if session.Token != token {
		log.Printf("[FetchAndValidateSession] - Token mismatch: expected %s, got %s\n", token, session.Token)
		return nil, fmt.Errorf("[Session] - token mismatch")
	}
	if session.OperatorIdentifier.OperatorName == "TestOp" {
		return session, nil
	}
	//log.Printf("[FetchAndValidateSession] - Token validation successful\n")
	if session.IsTokenExpired() {
		log.Printf("[FetchAndValidateSession] - Token expired\n")
		return nil, fmt.Errorf("[Session] - token expired")
	}
	//log.Printf("[FetchAndValidateSession] - Session validation successful: %+v\n", session)
	return session, nil
}

func FetchWalletBallance(session *models.Session, redis *redisdb.RedisClient) (int64, error) {
	module := interfaces.OperatorModules[session.OperatorIdentifier.OperatorName]
	walletBalance, err := module.HandleFetchWalletBalance(*session, redis)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch wallet : %v", err)
	}
	return walletBalance, nil
}

func CreatePlayer(redis *redisdb.RedisClient, session *models.Session) (*models.Player, bool, bool, error) {
	var player *models.Player
	var wasdisconnected bool
	wasdisconnected = false
	// We will just check if the player that connected is part of our disconnected players, this is a list of in game players that disconnected.
	// players not in game will just be deleted, and recreated when they are reconnected.
	discPlayer := redis.GetDisconnectedPlayerData(session.ID)
	if discPlayer != nil {
		wasdisconnected = true
		player = &models.Player{
			ID:                 discPlayer.ID,
			Token:              discPlayer.Token,
			Name:               discPlayer.Name,
			SelectedBet:        discPlayer.SelectedBet,
			SessionID:          discPlayer.SessionID,
			Currency:           session.Currency,
			Status:             models.StatusInGame,
			GameID:             discPlayer.GameID,
			OperatorIdentifier: session.OperatorIdentifier,
		}
		redis.AddPlayer(player)
		return player, wasdisconnected, false, nil
	}

	discPlayer = redis.GetDisconnectedInQueuePlayerData(session.ID)
	if discPlayer != nil {
		wasdisconnected = true
		player = &models.Player{
			ID:                 discPlayer.ID,
			Token:              discPlayer.Token,
			Name:               discPlayer.Name,
			SelectedBet:        discPlayer.SelectedBet,
			SessionID:          discPlayer.SessionID,
			Currency:           session.Currency,
			Status:             discPlayer.Status,
			RoomID:             discPlayer.RoomID,
			GameID:             "",
			OperatorIdentifier: session.OperatorIdentifier,
		}
		redis.AddPlayer(player)
		return player, wasdisconnected, true, nil
	}

	existingPlayer, _ := redis.GetPlayer(session.ID)
	if existingPlayer != nil {
		log.Println("Session with active player.")
		return nil, wasdisconnected, false, fmt.Errorf("session with active player")
	}

	newPlayer := &models.Player{
		ID:                 session.ID,
		Token:              session.Token,
		Name:               session.PlayerName,
		SessionID:          session.ID,
		Currency:           session.Currency,
		Status:             models.StatusOnline,
		OperatorIdentifier: session.OperatorIdentifier,
	}
	player = newPlayer
	//redis.AddPlayer(player) // Since its a new player, we add it to redis.

	redis.AddPlayer(player)
	return player, wasdisconnected, false, nil
}
