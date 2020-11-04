from array import array
import ipaddress
from datetime import datetime
import time

def SerializeAddress(listenAddress):
    """
    Serialize the subscription request message before sending it to the publisher
    :param listenAddress: subscriber's litening address
    """
    portArray = array('H', [listenAddress[1]])
    ipInInt = int(ipaddress.IPv4Address(listenAddress[0]))

    ipInBytes = ipaddress.v4_int_to_packed(ipInInt)
    portArray.byteswap()
    portInBytes = portArray.tobytes()

    serializedAddress = ipInBytes + portInBytes
    return serializedAddress
            
def DeserializeMessage(messageSequence):
    """
    deserialize the message from bytes
    :param messageSequence: byte array received from the publisher
    """
    global latestTimestamp
    messageDict = []
    for d in range(0,len(messageSequence),32):      ############################
        end = d + 32
        b = messageSequence[d:end]
        dict = {}
        timestamp = array('Q')
        timestamp = int.from_bytes(b[0:8], byteorder='big')
        dict['timestamp'] = timestamp
        c1 = b[8:11]
        c2 = b[11:14]
        curr1 = c1.decode()
        curr2 = c2.decode()
        dict['currency1'] = curr1
        dict['currency2'] = curr2
        p = array('d')
        p.frombytes(b[14:22])
        dict['price'] = p[0]
        messageDict.append(dict)
    return messageDict

