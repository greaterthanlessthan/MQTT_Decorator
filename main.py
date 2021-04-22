import time
import warnings
# import traceback
import paho.mqtt.client as mqtt

warnings.simplefilter("always")  # Always show warnings

GLOBAL_SUBSCRIBERS = {}


def note_subscribers(topic, subscriber) -> None:
    """
    Appends to GLOBAL_SUBSCRIBERS

    Objects will be added to that dictionary as keys. Their attributes will then be accessed dynamically by on_message

    :param topic: The MQTT topic
    :param subscriber: The class object that will receive updates when that topic is updated
    :return: None
    """
    try:
        subscriber.__dict__[topic]
    except KeyError:  # object was not given subscription
        warnings.warn(f"Wrong object sent to note_subscribers. "
                      f"Got {subscriber} which does not {topic} as an attribute"
                      f"which should be set in MQTTConnect")
    except AttributeError:  # object does not have __dict__ method
        warnings.warn(f"It looks like the wrong object was sent to note_subscribers. "
                      f"Got {subscriber} which does not have a __dict__ attribute, so it wasn't even a class instance")
    else:
        # create instance as a key, and note that it's subscribed to subscription
        GLOBAL_SUBSCRIBERS[subscriber] = topic
        print(f"Added {type(subscriber).__name__} to global_subscribers as subscribing to {topic}")


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
    for subscriber, topic in GLOBAL_SUBSCRIBERS.items():
        if topic == message.topic:
            # set the value of the subscriber's attribute in the SubscribedTopic subclass
            setattr(topic_subclass := subscriber.__dict__[topic], "_value", str(message.payload.decode("utf-8")))

            # call the method of that object
            try:
                topic_subclass.called_from_on_message()
            except AttributeError:
                warnings.warn(f"{subscriber}.{topic} "
                              f"didn't have a .called_from_on_message method. "
                              f"Is it not an instance of SubscribedTopic?")


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


def is_float(value_arg):
    try:
        float(value_arg)
        return True
    except ValueError:
        return False


class TopicHandler:
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
        return float(self._value) == other if is_float(self._value) else self._value == str(other)

    def __lt__(self, other):
        return float(self._value) < other if is_float(self._value) else self._value < str(other)

    def __le__(self, other):
        return float(self._value) <= other if is_float(self._value) else self._value <= str(other)

    def __gt__(self, other):
        return float(self._value) > other if is_float(self._value) else self._value > str(other)

    def __ge__(self, other):
        return float(self._value) >= other if is_float(self._value) else self._value >= str(other)

    def __ne__(self, other):
        return float(self._value) != other if is_float(self._value) else self._value != str(other)

    def __hash__(self):  # good practice when defining __eq__
        return hash(self._value)

    """
    Value property
    """
    # this doesn't work. o.TopicHandlerInstance just gets set to the value
    # def __set__(self, instance, value_arg):
    #     if self.publish(value_arg) == 0:
    #         self._value = value_arg
    #         self.last_publish_time = time.time()

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

    def called_from_on_message(self):
        # Todo: history into a database
        self.on_change()
        self.last_message_time = time.time()

    def on_change(self):
        # message for on_change method that was not redefined
        print(f"{self._topic} was updated, received {self._value} and not doing anything,\n"
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
    def __init__(self, subscriptions=None, publications=None):
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
        """
        # check if subscriptions is what we want
        try:
            if subscriptions is not None:
                iter(subscriptions)  # check if iterable if not default
        except TypeError:
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
        except TypeError:
            raise UserWarning(f"Iterable was not sent to MQTTConnect publications. Got {publications}")
        else:
            if type(publications) == str:
                self.publications = [publications]  # handle instance of lone string
            else:
                self.publications = publications  # final result: an iterable not including a string, or None

    # called when decorating a class
    def __call__(self, c):
        # set class attributes here

        # set classes subscriptions and publications
        c.subscriptions, c.publications = self.subscriptions, self.publications

        def better_str(c_self):
            return f"{type(c_self).__name__}"

        c.__str__ = better_str

        # create better dunder methods
        # create better string representation
        def better_repr(c_self):
            return f"This is an instance of {type(c_self).__name__}, which was decorated like so:" \
                   f"\n\n" \
                   f"@MQTTConnect(subscriptions={c.subscriptions}, publications={c.publications})\n" \
                   f"class {type(c_self).__name__}({c_self.__class__.__bases__[0].__name__}):"

        c.__repr__ = better_repr

        def setattr_to_publish(c_self, name, value):
            if name in c_self.__dict__:  # if there's no attribute of this name, set it like normal
                if type(c_self.__dict__[name]) == TopicHandler:  # but if there is, see if it's a TopicHandler
                    c_self.__dict__[name].value = value  # if it is instead of overwriting it, call .value
                    # which will try to publish the message, allowing for easy syntax of "class.Handler = value"
                else:
                    super(c, c_self).__setattr__(name, value)  # if it's not, set it like normal
            else:
                super(c, c_self).__setattr__(name, value)

        c.__setattr__ = setattr_to_publish

        def wrapper(*args, **kwargs):
            # create object
            o = c(*args, **kwargs)

            # set object attributes
            if o.subscriptions is not None:
                for subscription in o.subscriptions:
                    # subscribe to topics
                    result, mid = GLOBAL_CLIENT.subscribe(subscription)

                    # make topic instance
                    var_name = subscription.replace("/", "_")
                    try:
                        t = TopicHandler(subscription)
                        setattr(t, "_parent", str(o))
                        setattr(t, "_is_subscribed", True)

                        # give object an attribute with name equal to subscription
                        setattr(o, var_name, t)
                    except SyntaxError:
                        warnings.warn(f"{var_name} is not a valid variable name.")

                    # check success
                    if result != mqtt.MQTT_ERR_SUCCESS:
                        # warn that subscription wasn't successful
                        warnings.warn(f"Could not subscribe to {subscription}. Message ID: {mid}. Error code {result}")
                    else:
                        # add class to the global subscriber list on success
                        note_subscribers(subscription, o)

            if self.publications is not None:
                for publication in o.publications:
                    var_name = publication.replace("/", "_")
                    # don't make a TopicHandler instance if it was already done
                    if publication in o.subscriptions:
                        existing_topic_handler = o.__dict__[var_name]
                        setattr(existing_topic_handler, "_can_publish", True)
                    else:
                        # make topic instance
                        t = TopicHandler(publication)
                        setattr(t, "_parent", str(o))
                        setattr(t, "_can_publish", True)

                        # give object an attribute with name equal to subscription
                        setattr(o, var_name, t)

            # standard method name
            try:
                o.on_decorate()
            except AttributeError:
                pass

            return o
        return wrapper


@MQTTConnect(subscriptions="TEMPERATURE", publications=["TEMPERATURE", "AIR_COND/SOUTH"])
class TemperatureWatcher(object):
    # these help with type completion and will be added by MQTTConnect
    TEMPERATURE: TopicHandler
    AIR_COND_SOUTH: TopicHandler  # slashes are replaced with _
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
            self.AIR_COND_SOUTH = "ON"
        else:
            self.AIR_COND_SOUTH = "OFF"


# begin client
GLOBAL_CLIENT = start_client(message_func=on_message, broker_arg="192.168.0.28", port=1883)


cloo = TemperatureWatcher(72)

test_val = 60
while True:
    time.sleep(1)
    cloo.TEMPERATURE.value = test_val
    test_val += 6
    print(cloo.AIR_COND_SOUTH)
    print(repr(cloo))
