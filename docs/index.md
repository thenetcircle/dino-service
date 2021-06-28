# Dino

Service for distributed notifications using MQTT over websockets or TCP streams.

TODO: update arch image to current setup

![Sending a message](images/send-to-group.jpg)

1. User sends a message in a one-to-one/group chat,
2. Backend checks business logic and rules, then calls Dino Server REST API to save and broadcast a new message,
3. Dino Server saves the message in Cassandra,
4. Dino Server sends a broadcast event to the relevant MQTT topics (every user has their own topic),
5. Clients subscribing to their topics receives the event.

![Storage](images/storage.png)
