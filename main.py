import time
import warnings
import traceback

warnings.simplefilter("always")

"""
# executes once, when we get an argument
def subscribe_to(subscription, broker_arg='127.0.0.1', port=1883):
    """"""
    Needs to be outer decorator when used with publish_to

    Subscribes to topic (subscription argument) and gets result of subscribe request.
    Adds attribute of name <subscription argument> to class instance. Also adds attribute
    with name subscription with value <subscription argument>.

    Also adds attributes:
            o._subscription_result = result
            o._subscription_mid = mid
            o._subscription_broker = broker_arg
            o._subscription_port = port

    And finally, on successful subscribe, adds attribute client = global_client, then adds object to
    global_subscribers list

    :param subscription:
    :param broker_arg:
    :param port:
    :return:
    """"""
    # start client if it hasn't already been started
    start_client(on_message, broker_arg, port)

    # executes once, when we decorate a function
    def middle(c):
        print("here")

        pass

        # executes each time the function/class is ran
        def wrapper(*args, **kwargs):
            # subscribe to topic
            print(f"{str(c)} is subscribing to topic {subscription}")
            result, mid = global_client.subscribe(subscription)

            # create the class object
            o = c(*args, **kwargs)

            # create an attribute with the name of the subscription. This will be set by the on_message function
            # when it references the global_subscribers
            setattr(o, subscription, None)

            # retain the text of the subscription
            o.subscription = subscription

            # give class the subscription results and information
            o._subscription_result = result
            o._subscription_mid = mid
            o._subscription_broker = broker_arg
            o._subscription_port = port

            # check success
            if result == 0:
                # add the object to the dict of subscribers so it's known that it's subscribed
                note_subscribers(subscription, o)

                # give object an local variable that references the global_client
                o.client = global_client
            else:
                warnings.warn(f"Subscription to {subscription} with MID {mid} was not successful.")

            return o
        return wrapper
    return middle
    
    # executes once, when we get an argument
def publish_to(publication, broker_arg='127.0.0.1', port=1883):
    """"""
    Needs to be the inner wrapper when used with subscribe_to.

    For use on a class. Adds publish method, client, and publication to class.
    Adds _publication_broker and _publication_port to instance

    :param publication: the topic to publish to
    :param broker_arg: for setting up the client if not already done
    :param port: for setting up the client if not already done
    :return:
    """"""
    # start client if it hasn't already been started
    start_client(on_message, broker_arg, port)

    # executes once, when we decorate a function
    def middle(c):
        def publish(self, payload=None, qos=0, retain=False):
            self.client.publish(self.publication, payload=payload, qos=qos, retain=retain)

        if isinstance(c, type(subscribe_to)):
            raise UserWarning("publish_to needs to be the inner decorator when used with subscribe_to")

        # add the publish class method
        # for this to work, publish_to needs to be the inner decorator for direct access to class
        c.publish = publish

        # give the object access to the client with self.client
        c.client = global_client

        # retain the text of the publication
        c.publication = publication

        # executes each time the function/class is ran
        def wrapper(*args, **kwargs):
            # create the class object
            o = c(*args, **kwargs)

            # give class the subscription results and information
            o._publication_broker = broker_arg
            o._publication_port = port

            return o
        return wrapper
    return middle
    
    # this executes first when class is instantiated.
# c becomes <function subscribe_to.<locals>.middle.<locals>.wrapper at 0x000001B511300CA0>
# which is then called with o = c(). Inside subscribe_to, c is the class, and o becomes the instance
# subscribe_to appends attributes to o, then passes it back to publish_to as a class instance
#@publish_to("TEMPERATURE")
# this executes first when decorator is applied. c is the class itself. It can append methods and attributes
# to the class itself. publish_to only gets this function as c in middle
#@subscribe_to("TEMPERATURE")
#class temp_watcher(object):
#    def __init__(self):
#        pass


# order matters when added methods to classes
"""


class MQTTConnect(object):
    """
    Must be used as decorator with arguments. Intended for use on classes.

    syntax:

    @MQTTConnect(subscriptions=["topic1", "topic2"], publications=["topic1", "topic3"])
    class my_class(obj)

    is equivelenant to:

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
                    result, mid = o._client.subscribe(subscription)
                    # check success
                    if result == 0:
                        setattr(o, subscription, None)
                        self.note_subscribers(subscription, o)
                    else:
                        warnings.warn(f"Could not subscribe to {subscription}. Message ID: {mid}")
                        traceback.print_stack()

            if self.publications is not None:
                for publication in o.publications:
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
