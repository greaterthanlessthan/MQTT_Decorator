from paho.mqtt.client import MQTT_ERR_SUCCESS, Client as MQTTClient
from warnings import warn
from time import time


class Topic(object):
    """
    Class for handling topics
    """

    _value = "NO_MESSAGE_RECEIVED"  # the value of the topic
    _parent: str  # The parent class, used for warnings
    _listed_as_sub_at: dict  # where this class is listed as a subscriber
    _client: MQTTClient  # client being used for the instance

    # whether or not this can publish, whether or not the instance is a subscriber. Will be set in MQTT_Handler
    _can_publish = _is_subscribed = False

    last_message_time = last_publish_time = 0.0  # last subscription and publication message time in seconds

    def __init__(self, topic_string):
        self._topic = topic_string
        self._val_try_float = lambda: self._safe_float(self._value)

    def __str__(self):
        return str(self._value)

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
        # as float returns float object if possible, otherwise string
        return self._val_try_float() == other

    def __lt__(self, other):
        return self._val_try_float() < other

    def __le__(self, other):
        return self._val_try_float() <= other

    def __gt__(self, other):
        return self._val_try_float() > other

    def __ge__(self, other):
        return self._val_try_float() >= other

    def __ne__(self, other):
        return self._val_try_float() != other

    def __hash__(self):
        # THIS NEEDS TO BE UNIQUE FOR USING IT AS A KEY IN SUB_DICT TO WORK
        return hash(self._parent + self._topic + str(self._client))

    """
    Value property
    """
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value_arg):
        if self._publish(value_arg) == 0:
            self._value = value_arg
            self.last_publish_time = time()

    @value.getter
    def value(self):
        return self._value

    @staticmethod
    def _safe_float(value_arg):
        try:
            return float(value_arg)
        except ValueError:
            return value_arg

    def _called_from_on_message(self):
        self.on_change()
        self.last_message_time = time()

    def on_change(self):
        # message for on_change method that was not redefined
        warn(f"{self._topic} was updated, received {self._value} and not doing anything,\n"
             f"redefine {self._parent}.{self._topic}.on_change with a function that takes no arguments.")

    def _publish(self, payload=None, qos=0, retain=False):
        if not self._can_publish:
            print(f"{repr(self)} doesn't have permission to publish to topic {self._topic}.")
        else:
            result, mid = self._client.publish(topic=self._topic, payload=payload, qos=qos, retain=retain)
            if result != MQTT_ERR_SUCCESS:
                warn(f"Publish message {mid} failed. Error code {result}. Tried to publish "
                     f"{payload} to {self._topic}.")
            return result


def _note_subscribers(topic: str, subscriber: Topic, sub_dict: dict) -> None:
    """
    Appends to sub_dict when called

    Objects will be added to that dictionary as keys. Their attributes will then be accessed dynamically by on_message

    :param topic: The MQTT topic
    :param subscriber: The class object that will receive updates when that topic is updated
    :param sub_dict: Dictionary to store subscribers in
    :return: None
    """
    assert type(subscriber) == Topic, f"It looks like the wrong object was sent to note_subscribers. "\
                                             f"Got {type(subscriber).__name__} but expected type TopicHandler"
    # create instance as a key, and note that it's subscribed to subscription
    # each topic handler only has one topic
    sub_dict[subscriber] = topic
    print(f"Added TopicHandler of {subscriber._parent} to subscribers dict as subscribing to {topic}")


def call_topic_handler(client, userdata, message, sub_dict: dict) -> None:
    """
    Dynamically sets values and calls methods of class decorated with MQTT_Handler

    :param client:
    :param userdata:
    :param message:
    :param sub_dict:
    :return:
    """
    # set the attribute of each subscriber that's subscribed to the topic
    for subscriber, topic in sub_dict.items():
        subscriber: Topic
        if message.topic == topic:
            # set the value of the subscriber's attribute in the TopicHandler class
            # set _value as setting value will attempt to publish
            setattr(subscriber, "_value", str(message.payload.decode("utf-8")))
            subscriber._called_from_on_message()


class Connect(object):
    def __init__(self, client: MQTTClient,
                 sub_dict: dict,
                 subscriptions: list = None,
                 publications: list = None
                 ):
        """
        Must be used as decorator with arguments. Intended for use on classes.

        Sets class attributes to decorated object:
            .publications, .subscriptions as a list of the arguments received
            .<subscription> for each string in subscriptions argument, which will automatically update when a message
                is received

            .publish method which publishes a topic if it is set as a topic. Otherwise warns.

        syntax:

        @MQTT_Handler(subscriptions=["topic1", "topic2"], publications=["topic1", "topic3"])
        class my_class(obj)


        :param subscriptions: the topics to subscribe to
        :param publications: the topics the object is allowed to publish to
        """
        # arg checking
        assert type(client) == MQTTClient, f"Argument client was not of type {type(MQTTClient)}"
        self.client = client

        assert type(sub_dict) == dict
        self.sub_dict = sub_dict

        # check if subscriptions and publications is what we want
        self.subscriptions = self._check_pub_sub_arg(subscriptions)  # get an iterable not including a string, or None
        self.publications = self._check_pub_sub_arg(publications)  # get an iterable not including a string, or None

    @staticmethod
    def _var_name(string: str) -> str:
        # check if replacing / is enough for valid variable name
        assert (var_name := string.replace("/", "_")).isidentifier(), f"{var_name} is not a valid variable name."
        return var_name

    @staticmethod
    def _check_pub_sub_arg(iterable: list) -> list:
        # check if subscriptions is what we want

        if iterable is not None:
            iter(iterable)  # check if iterable if not default

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
                if type(c_self.__dict__[name]) == Topic:  # but if there is, see if it's TopicHandler
                    c_self.__dict__[name].value = value  # if it is, instead of overwriting it, call .value
                    # which will try to publish the message, allowing for easy syntax of "class.Handler = value"
                    return None
            old_setattr(c_self, name, value)  # if did not meet previous two if conditions, call old __setattr__ method

        old_setattr = c.__setattr__  # retain old __setattr__ method
        c.__setattr__ = setattr_to_publish  # make new one

        def wrapper(*args, **kwargs):
            o = c(*args, **kwargs)  # create object

            # set object attributes
            if o.subscriptions is not None:
                for sub in o.subscriptions:
                    setattr(t := Topic(sub), "_parent", type(o).__name__)  # make topic instance
                    setattr(t, "_is_subscribed", True)
                    setattr(t, "_client", self.client)
                    setattr(o, self._var_name(sub), t)  # give object TopicHandler attribute

                    result, mid = self.client.subscribe(sub)  # subscribe to topics

                    if result != MQTT_ERR_SUCCESS:  # check subscribe success
                        # warn that subscription wasn't successful
                        warn(f"Could not subscribe to {sub}. Message ID: {mid}. Error code {result}")
                    else:
                        # add class to the global subscriber list on success
                        _note_subscribers(sub, getattr(o, self._var_name(sub)), self.sub_dict)
                        setattr(t, "_listed_as_sub_at", self.sub_dict)

            if self.publications is not None:
                for pub in o.publications:
                    if pub in o.subscriptions:  # don't make a new TopicHandler instance if it was already done
                        setattr(o.__dict__[self._var_name(pub)], "_can_publish", True)
                    else:
                        setattr(t := Topic(pub), "_parent", type(o).__name__)  # make topic instance
                        setattr(t, "_can_publish", True)
                        setattr(t, "_client", self.client)
                        setattr(o, self._var_name(pub), t)  # give object TopicHandler attribute

            # standard method name
            try:
                o.on_decorate()
            except AttributeError:
                pass

            return o
        return wrapper
