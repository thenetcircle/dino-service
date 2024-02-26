# Public Groups

There's three types of groups:

```python
class GroupTypes:
    GROUP: Final = 0
    ONE_TO_ONE: Final = 1
    PUBLIC_GROUP: Final = 2
```

Public groups are groups that are open to everyone. They are discoverable and joinable by anyone. Public groups are 
useful for communities, forums, and other open discussions.

## User Statistics

When calling the `POST /v1/userstats/{user_id}` API, the `group_amount` and `unread_groups_amount` is a combination of 
public groups and private groups:

```json
{
  "...": "...",
  "group_amount": 0,
  "unread_groups_amount": 0
}
```

## Group creation

When a user creates a public group, the following happens:

TODO

## Group deletion

When a user deletes a public group, the following happens:

TODO

## Group joining

When a user joins a public group, the following happens:

TODO

## Group leaving

When a user leaves a public group, the following happens:

TODO

## Group details

When a user requests details of a public group, the following happens:

TODO

## Group members

When a user requests members of a public group, the following happens:

TODO

## Group messages

When a user requests messages of a public group, the following happens:

TODO

## Group events

TODO

## Group settings

TODO
