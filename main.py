import time
import warnings
import traceback

warnings.simplefilter("always")


class MQTTConnect(object):
    """
    Must be used as decorator with arguments. Intended for use on classes.

    syntax:

    @MQTTConnect(subscriptions=["topic1", "topic2"], publications=["topic1", "topic3"])
    class my_class(obj)

    is equivalent to:

    class my_class(obj)

    my_class = MQTTConnect(subscriptions=["topic1", "topic2"], publications=["topic1", "topic3"])(my_class)
    """

    # called when arguments are passed. This class doesn't work without arguments
    def __init__(self, subscriptions=None, publications=None, broker="127.0.0.1", port=1883):
        # check if subscriptions is what we want
        try:
            if subscriptions is not None:
                iter(subscriptions)  # check if iterable if not default
        except TypeError as e:
            raise UserWarning(f"Iterable was not sent to MQTTConnect subscriptions. Got {subscriptions}")
        else:
            if type(subscriptions) == str:
                self.subscriptions = [subscriptions]  # handle instance of lone string
            else:
                self.subscriptions = subscriptions  # final result: an iterable not including a string, or None

        # check if publications is what we want
        try:
            if publications is not None:
                iter(publications)  # check if iterable if not default
        except TypeError as e:
            raise UserWarning(f"Iterable was not sent to MQTTConnect publications. Got {publications}")
        else:
            if type(publications) == str:
                self.publications = [publications]  # handle instance of lone string
            else:
                self.publications = publications  # final result: an iterable not including a string, or None

        # save these arguments in instance
        self._broker = broker
        self._port = port

        # start client if it hasn't already been started
        self.start_client(message_func=self.on_message, broker_arg=broker, port=port)

    # called when decorating a class
    def __call__(self, c):
        # set class attributes here
        c._broker = self._broker
        c._port = self._port

        # set classes subscriptions and publications
        c.subscriptions = self.subscriptions
        c.publications = self.publications

        # create class publish method
        def publish(c_self, topic, payload=None, qos=0, retain=False):
            if topic not in c.publications:
                warnings.warn(f"{c} doesn't have permission to publish to topic {topic}.")
                traceback.print_stack()
            else:
                global_client.publish(topic=topic, payload=payload, qos=qos, retain=retain)

        setattr(c, "publish", publish)

        def wrapper(*args, **kwargs):
            # create object
            o = c(*args, **kwargs)

            # set object attributes here
            # send object a reference to the running client
            o._client = global_client

            if o.subscriptions is not None:
                for subscription in o.subscriptions:
                    # subscribe to topics
                    result, mid = o._client.subscribe(subscription)

                    # check success
                    if result == 0:
                        # on success, give object an attribute with name equal to subscription
                        setattr(o, subscription, None)
                        self.note_subscribers(subscription, o)  # add class to the global subscriber list
                    else:
                        # warn that subscription wasn't successful
                        warnings.warn(f"Could not subscribe to {subscription}. Message ID: {mid}")
                        traceback.print_stack()

            if self.publications is not None:
                for publication in o.publications:
                    # not used at the moment
                    pass

            return o
        return wrapper

    @staticmethod
    def start_client(message_func, broker_arg, port):
        # create global_client if it hasn't been made yet
        if globals().get('global_client') is None:
            import paho.mqtt.client as mqtt
            print("Creating new MQTT client")

            # global to ensure only one client is made
            global global_client
            global_client = mqtt.Client("global_client")

            # define on_message func
            global_client.on_message = message_func

            print(f"Connecting to broker {broker_arg} at port {port}")

            # Todo: on_connect()
            global_client.connect(broker_arg, port=port)
            global_client.loop_start()
        else:
            pass

    @staticmethod
    def on_message(client, userdata, message):
        print("message received ", str(message.payload.decode("utf-8")))
        print("message topic=", message.topic)
        print("message qos=", message.qos)
        print("message retain flag=", message.retain)
        # set the attribute of each subscriber that's subscribed to the topic
        for subscriber, topic in global_subscribers.items():
            if topic == message.topic:
                setattr(subscriber, topic, str(message.payload.decode("utf-8")))

    @staticmethod
    def note_subscribers(subscription, obj):
        if globals().get('global_subscribers') is None:
            print("Creating dict of subscribers")
            global global_subscribers
            global_subscribers = {}
        # create instance as a key, and note that it's subscribed to subscription
        global_subscribers[obj] = subscription
        print(f"Added {obj} to global_subscribers as subscribing to {subscription}")


@MQTTConnect(subscriptions="TEMPERATURE", publications="TEMPERATURE")
class TemperatureWatcher(object):
    def __init__(self):
        pass

    def clear(self, topic):
        self.publish(topic, 0)


cloo = TemperatureWatcher()

while True:
    time.sleep(1)
    print(cloo.TEMPERATURE)
    cloo.publish("TEMPERATURE", "99")
