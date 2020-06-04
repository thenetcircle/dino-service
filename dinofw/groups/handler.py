class GroupHandler:
    def __init__(self, gn_env):
        self.env = gn_env

    def groups_for(self, user_id: str):
        # TODO: get from redis/cassandra
        return [
            {
                "id": "1ce84950-a633-11ea-b068-b7c52dcc3cff",
                "preview": "Batman: so you remeber Joker last night?",
                "name": "awesome people group",
                "last_user": "batman",
                "owner": "batman",
                "created": "2020-05-03T13:12:43Z",
                "updated": "2020-06-04T07:16:28Z",
                "users_list": [
                    {
                        "id": 1234,
                        "name": "foo",
                        "avatar": "https://example.com/image/1234.jpg",
                    },
                    {
                        "id": 4321,
                        "name": "bar",
                        "avatar": "https://example.com/image/4321.jpg",
                    },
                    {
                        "id": 5678,
                        "name": "baz",
                        "avatar": "https://example.com/image/5678.jpg",
                    },
                ],
            }
        ]
