# WebSocket Messages

Messages with the tag [BROADCAST] are sent periodically to eligible clients.

---

## Cliente

### Example 1: Join Queue

This is used to join the queue with a bid value.

```json
{
  "command": "join_queue",
  "value": 2
}
```

```json
{
  "command": "join_queue",
  "value": 0.5
}
```

### Example 2: Leave Queue

This is used to leave the queue.

```json
{
  "command": "leave_queue"
}
```

### Example 3: Send Message (NOT IMPLEMENTED)

Doesnt do anything.

```json
{
  "command": "send_message",
  "value": "msg"
}
```

### Example 4: Custom value (WIP)

Used to send more complex data, needs to be worked on.

```json
{
  "command": "custom_command",
  "value": { "key": "value" }
}
```

---

# Server

### Example 1: Connected

After the connecton is established, sent to the cliente.

```json
{
  "command": "connected",
  "value": {
    "player_name": "JohnDoe",
    "money": 1000.5
  }
}
```

### Example 2: Paired

When a match starts, the value represents the color, 1 = black, 0 = white.

```json
{
  "command": "paired",
  "value": {
    "color": 1,
    "opponent": "127.0.0.1:54918"
  }
}
```

### Example 3: [BRADCAST] Update wating queue total

So that the client knows how many players are in queue.

```json
{
  "command": "update_waiting_queue",
  "value": {
    "waiting_queue_size": 5
  }
}
```
