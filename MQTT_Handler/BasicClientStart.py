import paho.mqtt.client as mqtt
from time import time


def start_client(message_func=None,
                 connect_func=lambda c, u, f, rc: print(f"Connect returned with result code {rc}"),
                 broker_arg="127.0.0.1",
                 port=1883
                 ) -> mqtt.Client:
    """

    :param connect_func:
    :param message_func: the function to call when message is received
    :param broker_arg:
    :param port:
    :return:
    """
    # Todo: other features of paho.mqtt.client e.g.on_connect, security password
    # start if it hasn't been done yet
    print("Creating new MQTT client")
    print(f"Connecting to broker {broker_arg} at port {port}")

    # time.time() for unique client name
    client_name = str(time())
    client = mqtt.Client(client_name)

    # define message and connect functions
    client.on_message = message_func
    client.on_connect = connect_func

    client.connect(broker_arg, port=port)
    client.loop_start()

    return client
