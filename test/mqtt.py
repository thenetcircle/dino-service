from fastapi import FastAPI
from gmqtt import Client as MQTTClient
from gmqtt.mqtt.constants import MQTTv50
import time

app = FastAPI()


@app.get("/")
async def try_shit():
    client = MQTTClient("client-id")
    await client.connect("maggie-kafka-1.thenetcircle.lab", port=1883, version=MQTTv50)

    client.publish('1972', str(time.time()), qos=1, message_expiry_interval=10)
    await client.disconnect()

