import time
import warnings
from typing import Union

import MQTT_Handler  # this imports MQTTConnect and call_topic_handler which are both required
from MQTT_Handler.BasicClientStart import start_client

warnings.simplefilter("always")  # Always show warnings


# begin client
SUBSCRIBERS = {}
CLIENT = start_client(message_func=lambda c, u, m: MQTT_Handler.call_topic_handler(c, u, m, SUBSCRIBERS),
                      broker_arg="192.168.0.28", port=1883)


@MQTT_Handler.Connect(CLIENT,
                      SUBSCRIBERS,
                      subscriptions=["TEMPERATURE", "NUM_PRESS"],
                      publications=["TEMPERATURE", "AIR_COND/SOUTH"])
class TemperatureWatcher(object):
    # these help with type completion, and these methods will be added by MQTT_Handler
    # Union type hint helps with the expected value that will be sent to or received from the MQTT broker
    TEMPERATURE: Union[MQTT_Handler.Topic, float]
    AIR_COND_SOUTH: Union[MQTT_Handler.Topic, str]  # slashes are replaced with _
    NUM_PRESS: Union[MQTT_Handler.Topic, float]

    publications: list
    subscriptions: list

    def __init__(self, hold_temp):
        self.hold_temp = hold_temp

    # this method name is called by MQTT_Handler.Connect at the end of instantiation, but it doesn't need to be used
    # it's useful for accessing new methods which weren't available when __init__ was called
    def on_decorate(self):
        self.TEMPERATURE.on_change = self.hooray

    def hooray(self):
        print(f"Hooray! TEMPERATURE was updated to {self.TEMPERATURE}.")
        if self.TEMPERATURE > self.hold_temp:
            # TemperatureWatcher's __setattr__ method has been modified, so instead of overwriting methods of type
            # TopicHandler, it instead calls self.TopicHandler.value
            # PyCharm/other IDEs will warn about this since type was hinted earlier and it probably won't see the
            # modification to __setattr__. That warning was suppressed with the Union hint
            self.AIR_COND_SOUTH = "ON"
        else:
            self.AIR_COND_SOUTH = "OFF"


cloo = TemperatureWatcher(72)

test_val = 60.0
if __name__ == "__main__":
    while True:
        time.sleep(1)
        cloo.TEMPERATURE = test_val
        test_val += 6
        print(cloo.TEMPERATURE > 80)
        print(cloo.NUM_PRESS)
