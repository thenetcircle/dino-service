# Client usage

Dino is using MQTT version 5.

First connect to MQTT and subscribe to your User ID topic. ACLs on the MQTT server side is set to 
`read-only` on the user's own "user ID topic" only; trying to subscribe on another user ID topic
than the "username" a client successfully authenticated with will result in an error:

```javascript
const settings = {
    clientId: '1234',
    username: '1234',
    password: '<when hashed, must match stored value in redis that the mqtt server will check against>',
    clean: false,
    qos: 1
}
const client  = mqtt.connect('mqtt://host:port/mqtt', settings);

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
