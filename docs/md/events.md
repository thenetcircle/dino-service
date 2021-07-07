# MQTT Events

Dino is using VerneMQ to broadcast events to clients using MQTT version 5. Below are the possible 
events a client can receive, and when they are received.

For events, it doesn't matter if they're for a 1-to-1 conversation in a multi-user group, they 
look the same, but `group_type` will be different (if specified). 

## Possible values for fields

### `group_type`

* `0`: multi-user group,
* `1`: 1-to-1.

### `message_type`

* `0`: message,
* `1`: no thanks,
* `2`: no thanks (hide),
* `3`: image,
* `4`: greeter meeter auto,
* `5`: greeter meeter manual,
* `100`: action.

### `event_type` 

* `join`
* `leave`
* `group`
* `read`
* `edit`
* `attachment`
* `message`
* `action_log`
* `delete_attachment`
* `delete_message`

## A group you're part of has been created or updated

<span style="color:red"><b>Event schema not yet finalized.</b></span>

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

## New message in a group you're part of

<span style="color:red"><b>Event schema not yet finalized.</b></span>

```json
{
    "event_type": "message",
    "group_id": "6cbb3265-2a7f-494d-92ad-f4503d55d49f",
    "sender_id": 1234,
    "file_id": null,
    "message_id": "c87efd18-8879-4c24-8b26-ccb7f40a0fe5",
    "message_payload": "<some content>",
    "message_type": 0,
    "created_at": 1597877384.794828
}
```

## New attachment in a group you're part of

<span style="color:red"><b>Event schema not yet finalized.</b></span>

```json
{
    "event_type": "attachment",
    "group_id": "6cbb3265-2a7f-494d-92ad-f4503d55d49f",
    "sender_id": 1234,
    "file_id": "f4503d55d49ff4503d55d49f",
    "message_id": "c87efd18-8879-4c24-8b26-ccb7f40a0fe5",
    "message_payload": "<some content>",
    "message_type": 3,
    "created_at": 1597877384.794828
}
```

## A message was edited

<span style="color:red"><b>Event schema not yet finalized.</b></span>

```json
{
    "event_type": "edit",
    "group_id": "6cbb3265-2a7f-494d-92ad-f4503d55d49f",
    "sender_id": 1234,
    "file_id": "f4503d55d49ff4503d55d49f",
    "message_id": "c87efd18-8879-4c24-8b26-ccb7f40a0fe5",
    "message_payload": "<some content>",
    "message_type": 0,
    "created_at": 1597877384.794828
}
```

## An action log was created

Action logs can be joins, leaves, highlight updated, group pinned etc.

<span style="color:red"><b>Event schema not yet finalized.</b></span>

```json
{
    "event_type": "action_log",
    "group_id": "6cbb3265-2a7f-494d-92ad-f4503d55d49f",
    "sender_id": 1234,
    "file_id": "f4503d55d49ff4503d55d49f",
    "message_id": "c87efd18-8879-4c24-8b26-ccb7f40a0fe5",
    "message_payload": "<some content>",
    "message_type": 100,
    "created_at": 1597877384.794828
}
```

## A user read a message/conversation

Read receipts are only broadcasted in 1-to-1 conversations, not in multi-user groups.

<span style="color:red"><b>Event schema not yet finalized.</b></span>

```json
{
    "event_type": "read",
    "group_id": "6cbb3265-2a7f-494d-92ad-f4503d55d49f",
    "user_id": 1234,
    "read_at": 1597877384.794828,
}
```

## Attachment(s) deleted

There may be multiple attachments connected to a single message, so `message_ids` and `file_ids` are lists.

```json
{
    "event_type": "delete_attachment",
    "created_at": 1597877384.794828,
    "group_id": "6cbb3265-2a7f-494d-92ad-f4503d55d49f",
    "message_ids": [
      "eeb0b18f-5485-4a24-8bd4-159301e4b554",
      "44b393c1-8ae2-4832-9770-07a043543423"
    ],
    "file_ids": [
      "238FD179EC794EFFBC033F491C335838",
      "95B1A97EDC6F4F5596D0158C55D17B1E"
    ]
}
```
