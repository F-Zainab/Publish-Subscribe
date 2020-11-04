"""
CPSC 5520, Seattle University
:Authors: Fariha Zainab
:Version: f19-02
:Assignment: Lab3-PUB_SUB
"""

import fxp_bytes_subscriber
import bellman_ford
import socket
from datetime import datetime
import time
import selectors

MICROS_PER_SECOND = 1_000_000
latestTimestamp = 0
REQUEST_SIZE =  512

def ReceiveMessage(publishedResponses):
    """
    register the listening socket in selector for read event
    Read the message received in bytes 
    """
    selector = selectors.DefaultSelector()
    selector.register(publishedResponses, selectors.EVENT_READ)
    while True:
        events = selector.select(timeout = 1.5)
        for key, mask in events:
            dataSequence, _address = publishedResponses.recvfrom(REQUEST_SIZE)
            UnmarshallMessages(dataSequence)

def UnmarshallMessages(dataSequence):
    """
    Takes the bytes array send by the publisher and deserialize it.
    :param dataSequence: bytes array of the published messege sent by the publisher
    """
    global latestTimestamp
    receivedMessage = fxp_bytes_subscriber.DeserializeMessage(dataSequence)
    for messages in receivedMessage:
        timestamp = messages['timestamp']
        if timestamp < latestTimestamp:
            print("ignoring out of order message")
            break
        else:
            t = time.ctime(timestamp/MICROS_PER_SECOND)
            Message = t + " " + messages['currency1'] + " " + messages['currency2'] + " " + str(messages['price'])
            latestTimestamp = timestamp
            print(Message)
    CheckArbitrage(receivedMessage)

def CheckArbitrage(receivedMessage):
    """
    Run the bellman ford algorithm on the received message to detect the negative cycle
    :param receivedMessage: list of dictionary of the currency with exchange rate
    """
    bellman = bellman_ford.ArbitrageDetector()
    for message in receivedMessage:
        bellman.ProcessPublishedPrice(message['timestamp'], message['currency1'], message['currency2'], message['price'])
    bellman.CheckForArbitrage()

def CreateListeningSocket():
    """
    Creating a listening socket for receiving the published messages
    :param listenAddress: Listening address of the subscriber to receive published message
    """
    listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listeningSocket.bind(('localhost', 0))
    listeningSocket.settimeout(1.5)
    return listeningSocket

if __name__ == "__main__":
    """
    creating socket to send the connection request to the publisher along with the subscription request message
    """
    publisherIP = 'cs2.seattleu.edu'
    publisherPort = 50403
    publisherAddress = (publisherIP, publisherPort)
    publishedResponses = CreateListeningSocket()
    listenAddress = publishedResponses.getsockname()
    subscriberSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    subscriptionRequestMessage = fxp_bytes_subscriber.SerializeAddress(listenAddress)
    subscriberSocket.sendto(subscriptionRequestMessage, publisherAddress)
    ReceiveMessage(publishedResponses)
    