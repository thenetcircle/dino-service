# MQTT Events

Dino is using VerneMQ to broadcast events to clients using MQTT version 5. Below are the possible 
events a client can receive, and when they are received.

For events, it doesn't matter if they're for a 1-to-1 conversation in a multi-user group, they 
look the same, but `group_type` will be different (if specified). 

Possible values for `group_type`:

* `0`: multi-user group,
* `1`: 1-to-1.

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
    "message_type": "text",
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
    "message_type": "image",
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
    "message_type": "text",
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
    "message_type": "text",
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
