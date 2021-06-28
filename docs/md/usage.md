# Client usage

First connect to MQTT and subscribe to your User ID topic:

```javascript
let settings = {
    clientId: '1234',
    username: '1234',
    clean: false,
    qos: 1
}
let client  = mqtt.connect('mqtt://host:port/mqtt', settings);

client.on('connect', function () {
    client.subscribe('1234', {qos: 1}, function (err) {
        if (err) {
            console.log(err);
        }
    })
});

client.on('message', function (topic, message) {
    console.log(message.toString());
});
```

Event when a group you're part of has been created or updated:

```json
{
    "event_type": "group",
    "group_id": "9d78163c-1144-485a-92c6-6015afc6cd40",
    "name": "a name",
    "created_at": 1597877421.453804,
    "updated_at": 1597877421.453804,
    "last_message_time": 1597877421.453804,
    "group_type": 0,
    "owner_id": 1234,
    "user_ids": [1234]
}
```

Event when a new message is sent to a group you're part of:

```json
{
    "event_type": "message",
    "group_id": "6cbb3265-2a7f-494d-92ad-f4503d55d49f",
    "sender_id": 1234,
    "message_id": "c87efd18-8879-4c24-8b26-ccb7f40a0fe5",
    "message_payload": "test message from rest api",
    "message_type": "text",
    "created_at": 1597877384.794828
}
```
