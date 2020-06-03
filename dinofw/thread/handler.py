class ThreadHandler:
    def __init__(self, gn_env):
        self.env = gn_env

    def threads_for(self, user_id: str):
        return [
            {
                "id": 0,
                "preview": "some group with baz",
                "last_user": "normaloaccount",
                "users_list": [{
                    "id": 1234,
                    "name": "foo",
                    "avatar": "https://example.com/image/1234.jpg"
                }, {
                    "id": 4321,
                    "name": "bar",
                    "avatar": "https://example.com/image/4321.jpg"
                }, {
                    "id": 5678,
                    "name": "baz",
                    "avatar": "https://example.com/image/5678.jpg"
                }]
            },
            {
                "id": 1,
                "preview": "bar",
                "last_user": "foo",
                "users_list": [{
                    "id": 1234,
                    "name": "foo",
                    "avatar": "https://example.com/image/1234.jpg"
                }, {
                    "id": 4321,
                    "name": "bar",
                    "avatar": "https://example.com/image/4321.jpg"
                }]
            }
        ]
