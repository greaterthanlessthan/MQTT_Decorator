import time
import warnings
# import traceback
import paho.mqtt.client as mqtt
from typing import Union
# import logging

warnings.simplefilter("always")  # Always show warnings

GLOBAL_SUBSCRIBERS = {}

test: int

def note_subscribers(topic: str, subscriber: object) -> None:
    """
    Appends to GLOBAL_SUBSCRIBERS

    Objects will be added to that dictionary as keys. Their attributes will then be accessed dynamically by on_message

    :param topic: The MQTT topic
    :param subscriber: The class object that will receive updates when that topic is updated
    :return: None
    """
    try:
        assert type(subscriber.__dict__[topic]) == TopicHandler
    except KeyError:  # object was not given subscription
        warnings.warn(f"Wrong object sent to note_subscribers. "
                      f"Got {subscriber} which does not have {topic} as an attribute"
                      f"which should be set in MQTTConnect")
        raise
    except AttributeError:  # object does not have __dict__ method
        warnings.warn(f"It looks like the wrong object was sent to note_subscribers. "
                      f"Got {subscriber} which does not have a __dict__ attribute, so it wasn't even a class instance")
        raise
    except AssertionError:
        warnings.warn(f"It looks like the wrong object was sent to note_subscribers. "
                      f"Got {subscriber}.{topic} which isn't an instance of TopicHandler")
        raise
    else:
        # create instance as a key, and note that it's subscribed to subscription
        if subscriber not in GLOBAL_SUBSCRIBERS:
            GLOBAL_SUBSCRIBERS[subscriber] = [topic]
        else:
            GLOBAL_SUBSCRIBERS[subscriber].append(topic)
        print(f"Added {type(subscriber).__name__} to GLOBAL_SUBSCRIBERS as subscribing to {topic}")


def on_message(client, userdata, message) -> None:
    """
    Dynamically sets values and calls methods of class decorated with MQTTConnect

    :param client:
    :param userdata:
    :param message:
    :return:
    """
    print("message received ", str(message.payload.decode("utf-8")))
    # print("message topic=", message.topic)
    # print("message qos=", message.qos)
    # print("message retain flag=", message.retain)
    # set the attribute of each subscriber that's subscribed to the topic
    for subscriber, topic_list in GLOBAL_SUBSCRIBERS.items():
        if message.topic in (t_l := topic_list):
            # set the value of the subscriber's attribute in the SubscribedTopic subclass
            # set _value as setting value will attempt to publish
            topic = t_l[t_l.index(message.topic)]
            setattr(topic_subclass := subscriber.__dict__[topic],
                    "_value", str(message.payload.decode("utf-8")))

            # call the method of that object
            try:
                topic_subclass.called_from_on_message()
            except AttributeError:
                warnings.warn(f"{subscriber}.{topic} "
                              f"didn't have a .called_from_on_message method. "
                              f"Is it not an instance of SubscribedTopic?")
                raise


def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("$SYS/#")


def start_client(message_func=on_message, connect_func=on_connect, broker_arg="127.0.0.1", port=1883) -> mqtt.Client:
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
    client_name = str(time.time())
    client = mqtt.Client(client_name)

    # define message and connect functions
    client.on_message, client.on_connect = message_func, connect_func

    client.connect(broker_arg, port=port)
    client.loop_start()

    return client


class TopicHandler(object):
    """
    Class for handling topics
    """

    _value = "not set"  # the value of the topic
    _parent = None  # The parent class, used for warnings

    # whether or not this can publish, whether or not the instance is a subscriber. Will be set in MQTTConnect
    _can_publish = _is_subscribed = False

    # last subscription and publication message time in seconds
    last_message_time = last_publish_time = 0.0

    def __init__(self, topic_string):
        self._topic = str(topic_string)

    def __str__(self):
        return self._value

    def __repr__(self):
        pub_string = "publishing" if self._can_publish else ""
        sub_string = "subscribing" if self._is_subscribed else ""
        and_string = " and " if sub_string != "" and pub_string != "" else ""

        return f"The subscription subclass for {self._parent}," \
               f" {sub_string}{and_string}{pub_string} to topic {self._topic}"

    """
    Define comparison dunder methods
    """
    def __eq__(self, other):
        return self.as_float == other

    def __lt__(self, other):
        return self.as_float < other

    def __le__(self, other):
        return self.as_float <= other

    def __gt__(self, other):
        return self.as_float > other

    def __ge__(self, other):
        return self.as_float >= other

    def __ne__(self, other):
        return self.as_float != other

    def __hash__(self):  # good practice when defining __eq__
        return hash(self.as_float)

    """
    Value property
    """
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value_arg):
        if self.publish(value_arg) == 0:
            self._value = value_arg
            self.last_publish_time = time.time()

    @value.getter
    def value(self):
        return self._value

    @staticmethod
    def _safe_float(value_arg):
        try:
            return float(value_arg)
        except ValueError:
            return value_arg

    @property
    def as_float(self):
        return self._safe_float(self._value)

    @as_float.getter
    def as_float(self):
        return self._safe_float(self._value)

    def called_from_on_message(self):
        self.on_change()
        self.last_message_time = time.time()

    def on_change(self):
        # message for on_change method that was not redefined
        warnings.warn(f"{self._topic} was updated, received {self._value} and not doing anything,\n"
                      f"redefine {self._parent}.{self._topic}.on_change with a function that takes no arguments.\n"
                      f"You may also want to define other attributes from the SubscribedTopic class.")

    def publish(self, payload=None, qos=0, retain=False):
        if not self._can_publish:
            warnings.warn(f"{repr(self)} doesn't have permission to publish to topic {self._topic}.")
        else:
            result, mid = GLOBAL_CLIENT.publish(topic=self._topic, payload=payload, qos=qos, retain=retain)
            if result != mqtt.MQTT_ERR_SUCCESS:
                warnings.warn(f"Publish message {mid} failed. Error code {result}. Tried to publish "
                              f"{payload} to {self._topic}.")
            return result


class MQTTConnect(object):
    def __init__(self, client: mqtt.Client,
                 subscriptions: Union[list, set, tuple] = None,
                 publications: Union[list, set, tuple] = None,
                 change_repr: bool = True):
        """
        Must be used as decorator with arguments. Intended for use on classes.

        Sets class attributes to decorated object:
            .publications, .subscriptions as a list of the arguments received
            .<subscription> for each string in subscriptions argument, which will automatically update when a message
                is received

            .publish method which publishes a topic if it is set as a topic. Otherwise warns.

        syntax:

        @MQTTConnect(subscriptions=["topic1", "topic2"], publications=["topic1", "topic3"])
        class my_class(obj)

        :param subscriptions: the topics to subscribe to
        :param publications: the topics the object is allowed to publish to
        :param change_repr: normally True, set to False to prevent setting custom __repr__
        """
        # arg checking
        assert type(change_repr) == bool, f"Argument change_repr was not of type bool"
        self.change_repr = change_repr

        assert type(client) == mqtt.Client, f"Argument client was not of type {type(mqtt.Client)}"
        self.client = client

        # check if subscriptions and publications is what we want
        self.subscriptions = self._check_pub_sub_arg(subscriptions)  # get an iterable not including a string, or None
        self.publications = self._check_pub_sub_arg(publications)  # get an iterable not including a string, or None

    @staticmethod
    def _get_name(string: str) -> str:
        assert (var_name := string.replace("/", "_")).isidentifier(), f"{var_name} is not a valid variable name."
        return var_name

    @staticmethod
    def _check_pub_sub_arg(iterable: Union[list, set, tuple, str]) -> Union[list, None]:
        # check if subscriptions is what we want
        try:
            if iterable is not None:
                iter(iterable)  # check if iterable if not default
        except TypeError:
            warnings.warn(f"Iterable was not sent to MQTTConnect. Got {iterable}")
            raise
        else:
            # handle instance of lone string
            iterable_not_str = [iterable] if type(iterable) == str else iterable
            if iterable_not_str is not None:
                for string in iterable_not_str:
                    assert type(string) == str, f"publications and subscriptions arguments needs to consist " \
                                                f"of only strings. {string} was not a string."
            return iterable_not_str
            # final result: an iterable not including a string, or None

    # called when decorating a class
    def __call__(self, c):
        # set class attributes here

        # set classes subscriptions and publications
        c.subscriptions, c.publications = self.subscriptions, self.publications

        def setattr_to_publish(c_self, name, value):
            if name in c_self.__dict__:  # if there's an attribute of this name
                if type(c_self.__dict__[name]) == TopicHandler:  # but if there is, see if it's TopicHandler
                    c_self.__dict__[name].value = value  # if it is, instead of overwriting it, call .value
                    # which will try to publish the message, allowing for easy syntax of "class.Handler = value"
                    return None
            old_setattr(c_self, name, value)  # if did not meet previous two if conditions, call old __setattr__ method

        old_setattr = c.__setattr__  # retain old __setattr__ method
        c.__setattr__ = setattr_to_publish  # make new one

        def wrapper(*args, **kwargs):

            # create better dunder methods
            # create better string representation
            def better_repr(c_self):
                n = type(c_self).__name__
                base = c_self.__class__.__bases__[0].__name__
                return f"This is an instance of {n}, which was decorated like so:" \
                       f"\n\n" \
                       f"@MQTTConnect(subscriptions={c.subscriptions}, publications={c.publications})\n" \
                       f"class {n}({base}):\n" \
                       f"   ..." \
                       f"\n\n" \
                       f"It was initialized with {n}({args, kwargs})"

            c.__repr__ = better_repr if self.change_repr else c.__repr__  # write new repr if allowed

            # create object
            o = c(*args, **kwargs)

            # set object attributes
            if o.subscriptions is not None:
                for subscription in o.subscriptions:
                    result, mid = self.client.subscribe(subscription)  # subscribe to topics

                    var_name = self._get_name(subscription)  # check if replacing / is enough for valid variable name

                    setattr(t := TopicHandler(subscription), "_parent", type(o).__name__)  # make topic instance
                    setattr(t, "_is_subscribed", True)
                    setattr(o, var_name, t)  # give object an attribute with name equal to subscription

                    if result != mqtt.MQTT_ERR_SUCCESS:  # check subscribe success
                        # warn that subscription wasn't successful
                        warnings.warn(f"Could not subscribe to {subscription}. Message ID: {mid}. Error code {result}")
                    else:
                        note_subscribers(subscription, o)  # add class to the global subscriber list on success

            if self.publications is not None:
                for publication in o.publications:
                    var_name = self._get_name(publication)  # check if replacing / is enough for valid variable name

                    if publication in o.subscriptions:  # don't make a new TopicHandler instance if it was already done
                        setattr(o.__dict__[var_name], "_can_publish", True)
                    else:
                        setattr(t := TopicHandler(publication), "_parent", type(o).__name__)  # make topic instance
                        setattr(t, "_can_publish", True)
                        setattr(o, var_name, t)  # give object an attribute with name equal to subscription

            # standard method name
            try:
                o.on_decorate()
            except AttributeError:
                pass

            return o
        return wrapper


# begin client
GLOBAL_CLIENT = start_client(message_func=on_message, broker_arg="192.168.0.28", port=1883)


@MQTTConnect(GLOBAL_CLIENT, subscriptions=["TEMPERATURE", "NUM_PRESS"], publications=["TEMPERATURE", "AIR_COND/SOUTH"])
class TemperatureWatcher(object):
    # these help with type completion, and these methods will be added by MQTTConnect
    # Union type hint helps with the expected value that will be sent to or received from the MQTT broker
    TEMPERATURE: Union[TopicHandler, float]
    AIR_COND_SOUTH: Union[TopicHandler, str]  # slashes are replaced with _
    NUM_PRESS: Union[TopicHandler, float]

    publications: list
    subscriptions: list

    def __init__(self, hold_temp):
        self.hold_temp = hold_temp

    # this method name is called by MQTTConnect at the end of instantiation, but it doesn't need to be used
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
while True:
    time.sleep(1)
    cloo.TEMPERATURE = test_val
    test_val += 6
    # cloo._topic_qos0 = 60
    print(cloo.TEMPERATURE > 80)
    print(cloo.NUM_PRESS)
    # print(repr(cloo))
