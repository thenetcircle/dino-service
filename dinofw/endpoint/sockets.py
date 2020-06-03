from dinofw.server import app, socketio
from dinofw import api


# no pre-processing for connect event
@socketio.on("connect", namespace="/ws")
@respond_with("gn_connect")
def connect() -> (int, None):
    return api.connect()
