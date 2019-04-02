"""A communications module using ZeroMQ.

Relies on a running instance of `MessageBroker` and allows an arbitrary number
of publishers to distribute data to an arbitrary number of subscribers,
filtering subscriptions by topics (which are just strings).

The system is robust enough to allow adding and removing publishers and
subscribers at any time, even the `MessageBroker` is allowed to disappear
temporarily. Of course this results in lost messages, but execution will
continue as soon as it has rejoined the network.

The `MessageBroker` class allows for dynamic discovery of publishers and
subscribers. It provides a frontend endpoint for publishers to connect to and a
backend endpoint for subscribers to connect to, which means that all parties
only need to know the location of this single component in the network to
exchange messages.

"""

import threading
import zmq


class MessageBroker(threading.Thread):
    """Message distribution from publishers to subscribers.

    Allows for dynamic discovery of publishers and subscribers. It provides a
    frontend endpoint for publishers to connect to and a backend endpoint for
    subscribers to connect to, which means that all parties only need to know
    the location of this single component in the network to exchange messages.

    This is a thread, so it needs to be started after instantiation. Since it
    will run forever, it behaves as a daemon thread. An application will
    terminate as soon as it doesn't have anything else to do other than running
    daemon threads. This means that:
        1. This thread doesn't need to be joined.
        2. If your application doesn't do anything else and you want to keep
           running this, you need to stop it from terminating.

    """

    def __init__(self, port_in, port_out):
        """Instantiate a new `MessageBroker`.

        Parameters
        ----------
        port_in : str
            The port for publishers to send messages to.
        port_out : str
            The port for subscribers to receive messages from.

        """
        threading.Thread.__init__(self)
        self.port_in = port_in
        self.port_out = port_out
        # Set as daemon so application can exit when only this remains running
        self.daemon = True

    def run(self):
        context = zmq.Context()
        # Bind to frontend and backend
        frontend = context.socket(zmq.XSUB)
        frontend.bind("tcp://*:{}".format(self.port_in))
        backend = context.socket(zmq.XPUB)
        backend.bind("tcp://*:{}".format(self.port_out))
        # Start proxying
        zmq.proxy(frontend, backend)


class Publisher:
    """Message publisher.

    Allows sending messages for a topic.

    """

    def __init__(self, address, port, topic):
        """Instantiate a new `Publisher`.

        Parameters
        ----------
        address : str
            The network address of the `MessageBroker`.
        port : str
            The port for the frontend of the `MessageBroker`.
        topic : str
            The topic under which messages should be sent.

        """
        self.topic = topic
        # Connect publishing socket to endpoint
        context = zmq.Context()
        self.socket = context.socket(zmq.PUB)
        self.socket.connect("tcp://{}:{}".format(address, port))

    def send(self, message):
        """Send a message for the topic of this `Publisher`.

        Parameters
        ----------
        message : str
            The message to be sent.

        """
        self.socket.send_string("{}!{}".format(self.topic, message))


class Subscriber(threading.Thread):
    """Message receiver.

    Allows receiving messages for a topic. It will filter messages by the topic
    and execute the given handler function, passing it the message.

    This is a thread, so it needs to be started after instantiation. Since it
    will run forever, it behaves as a daemon thread. An application will
    terminate as soon as it doesn't have anything else to do other than running
    daemon threads. This means that:
        1. This thread doesn't need to be joined.
        2. If your application doesn't do anything else and you want to keep
           running this, you need to stop it from terminating.

    """

    def __init__(self, address, port, topic, handler, args={}):
        """Instantiate a new `Receiver`.

        Parameters
        ----------
        address : str
            The network address of the `MessageBroker`.
        port : str
            The port for the backend of the `MessageBroker`.
        topic : str
            The topic used to filter messages.
        handler : fun
            Function taking a string (the message) and optionally some keyword
            arguments as arguments. Will be called when receiving a message.
        args : dict, optional
            The dictionary with keyword arguments which will be unpacked and
            passed to the handler function on every call.

        """
        threading.Thread.__init__(self)
        self.handler = handler
        self.args = args
        # Index of first characters in messages like "topic!message"
        self.msg_start = len(topic) + 1
        # Connect subscribing socket to endpoint
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)
        self.socket.connect("tcp://{}:{}".format(address, port))
        # Only receive messages starting with our topic
        self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        # Set as daemon so application can exit when only this remains running
        self.daemon = True

    def run(self):
        while True:
            msg_orig = self.socket.recv_string()
            # Remove the topic
            msg = msg_orig[self.msg_start:]
            # Invoke handler function with message
            self.handler(msg, **self.args)
