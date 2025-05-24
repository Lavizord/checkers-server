package messages

type CommandType string

const (
	ServerCommand    CommandType = "server"
	ClientCommand    CommandType = "client"
	BroadcastCommand CommandType = "broadcast"
)

type CommandInfo struct {
	Type CommandType
}

var validCommands = map[string]CommandInfo{
	"queue":       {Type: ClientCommand}, // This adds the player to a queue, there is a small issue with a single player in a queue, but i believe that has been handled.
	"ready_queue": {Type: ClientCommand}, // This allows the player to issue a ready when in a room. Opponent receives an opponent_ready message
	"leave_queue": {Type: ClientCommand}, // This allows the player to leave the queue.
	"leave_room":  {Type: ClientCommand}, // This allows the player to leave the room. The opponent gets placed in the Queue, and received a ready_queue message
	"leave_game":  {Type: ClientCommand}, // This allows the player to leave the game. The opponent wins the game
	"ping":        {Type: ClientCommand},
	"pong":        {Type: ServerCommand},
	"join_room":   {Type: ClientCommand}, // DEPRECATED
	"create_room": {Type: ClientCommand}, // DEPRECATED

	"move_piece":   {Type: ClientCommand}, // This is issued by the cliente to trigger the movement of a piece.
	"invalid_move": {Type: ServerCommand}, // This is issued by the cliente to trigger the movement of a piece.

	"message":                    {Type: ServerCommand}, // issues when a player connects.
	"connected":                  {Type: ServerCommand}, // issues when a player connects.
	"queue_confirmation":         {Type: ServerCommand}, // This confirms that the player was placed in Queue.
	"room_created":               {Type: ServerCommand}, // DEPRECATED
	"room_timer":                 {Type: ServerCommand},
	"room_failed_ready_check":    {Type: ServerCommand},
	"paired":                     {Type: ServerCommand}, // This lets players know they were paired.
	"opponent_ready":             {Type: ServerCommand}, // This lets the player know if the opponent is ready.
	"opponent_left_room":         {Type: ServerCommand}, // This is a message sent when a player leaves a room.
	"opponent_disconnected_game": {Type: ServerCommand}, // This is a message sent when a player disconnects from a game.
	"game_start":                 {Type: ServerCommand}, // This is the message sent with data for the game start.
	"game_reconnect":             {Type: ServerCommand}, // The same as game_start, but for a reconnect.
	"game_timer":                 {Type: ServerCommand}, // This is a timer sent to both players in a game.
	"game_over":                  {Type: ServerCommand}, // Sent when server detects a game over.
	"turn_switch":                {Type: ServerCommand}, // Sent when the server detects a turn switch.
	"balance_update":             {Type: ServerCommand}, // Sent when there is a change to a players money.

	"game_info": {Type: BroadcastCommand}, // Sent with generic game info to feed the clientes.
}
