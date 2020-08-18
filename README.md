# dino-framework

Framework for distributed notifications using websockets.

![Sending a message](docs/images/send-to-group.jpg)

1. User sends a message in a group chat,
2. Community checks business logic and rules, then calls Dino Server REST API to save and broadcast a new message,
3. Dino Server saves the message in Cassandra,
4. Dino Server sends a broadcast request (containing a list of all user IDs that are in the group) to a Redis Stream,
5. One of the Dino Clients that is subscribing to the stream group will received the broadcast request (all nodes are 
in the same consumer group and are thus load-balanced),
6. The Dino Client sends one copy of the message to each user's MQTT topic (every user has their own topic),
7. MQTT pushes the new message to each user in the group.

![Storage](docs/images/storage.png)
