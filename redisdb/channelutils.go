package redisdb

import "checkers-server/models"

func GetPlayerPubSubChannel(player models.Player) (string) {
	return "player:"+string(player.ID)
}
func GeneratePlayerRedisKey(player models.Player) (string) {
	return GetPlayerPubSubChannel(player)
}
func GenerateRoomRedisKey(room models.Room) (string) {
	return "room:"+string(room.ID)
}
func GenerateRoomRedisKeyById(roomId string) (string) {
	return "room:"+string(roomId)
}