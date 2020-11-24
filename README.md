# RFXcom

## Introduction
RFXcom has created a few transceivers for use in the 433 [MHz] RF band. As the name implies, a transceiver can both send and receive packets. Moreover, the RFXcom transceivers support multiple protocols, making them perfect devices to control multiple devices of multiple brands. Some domotica controllers, like domoticz, therefore also have a driver to access an RFXcom transceiver to acquire data and send commands using multiple protocols to multiple types of devices. However, even in these cases, only a single program has access, via a serial USB connection, to the transceiver itself.

Python script mqtt.rfxcom.py offers is a multiplexer, which accepts commands via MQTT and sends the response back via MQTT. The many-to-one and the one-to-many facilities of MQTT are used to offer independent and simultaneous access from multiple programs to the RFXcom transceiver. Moreover, the programs using the transceiver no longer need to run on the machine to which the transceiver is physically connected. Thus it becomes possible to have a program which receives and analyses the unsolicited responses received by the RFXcom transceiver to run on some machine in your (local) network while at the same time another program, on possibly another computer, is controlling the lights.

## Access
As multiple programs can access the transceiver, there must be some way to send the response back to the originator of the command. In version 0.10 of script mqtt.rfxcom.py, the method chosen is to prepend an identifier to the command, which is stripped before the command is send to the transceiver. Upon receipt of the response, the identifier is prepended to the response.

The structure of a command is '<Identifier>;<Command>' and similarly is the structure of a response '<Identifier>;<Response>'. The identifier should be a (small) string of characters which should not contain a semicolon. The command should not start with a byte containing the remaining length: the length is already known. (Thread Rfxcom, which is described below, takes care of the length byte.) Shown below is an example, in which a program, with identifier 'monzm', switches a light on. The response is an acknowledge from the transceiver that the command has been sent.

```
  20201124 065201 mqtt cmd <b'monzm;\x11\x00\x00\x02#\x04\x12\x01\x01\x0fP'>
  20201124 065202 mqtt rsp <b'monzm;\x02\x01\x00\x00'>
```

Note that if no response is received from the transceiver within 3.5 seconds, a 'negative acknowledge' (NAK) is generated. The content of this NAK is <b'\x02\x01\x00\x02'>

The typical flow of events for a source of commands is to subscribe to topic 'rfxcom/response', send a command, including it's identifier, to topic 'rfxcom/command' and wait for a response. If a response comes in with another identifier, it will be silently ignored. If a response comes in with the correct identifier, the identifier is stripped and the response is acted upon. (This method looks like Ethernet: a packet with a destination address matching the address of the network interface is accepted, a packet with another (unicast) destination address is silently ignored.)

## Script mqtt.rfxcom.py
Script mqtt.rfxcom.py maps a command received via topic 'rfxcom/command' onto a command send to the RFXcom transceiver. It maps the response from the transceiver onto a response on topic 'rfxcom/response', using the (source) identifier and the packet sequence number from the associated command. The script also maps any unsolicited response onto a message on topic 'rfxcom/message'. The latter does not contain a source identifier.

The script contains a thread named 'Dispatcher'. It takes a command, sends it to the transceiver and waits for the response. If no response comes in, a NACK response is generated. The next command is not send to the transceiver until a response is either received or generated. This thread thus implements a simple flow control mechanism, using a transmit window of only one packet.

The thread 'Dispatcher' also makes the sequence numbers between any source and this script and between this script and the transceiver independent of one another. It rewrites the sequence number in the commands send to the transceiver, and rewrites the sequence number in a response before sending it to it's source. The sequence number in an unsolicited response is not modified.

Below a top level view of the structure of the script is shown.

<img src="https://github.com/wnelis/RFXcom/blob/main/docs/tlad.png" >

Thread Mqtt initiates and maintains the connection with the MQTT broker. It moves packets between queues and the MQTT broker without interpretation or modification. Similarly, thread Rfxcom initiates and maintains the (serial) connection with the transceiver, and it moves packets between queues and the transceiver without interpretation or modification, however with one exception: a reset command is silently ignored. As a result thread Dispatcher does not have to take care of the state of the connection to either the transceiver or the broker and has a simple interface in both directions (a queue). The queues also act as a buffer, to smoothen the flow if necessary. 

## Modules
Script mqtt.rfxcom.py uses additional modules to implement a finite state machine, to create a watchdog timer and to create threads which can be stopped in a graceful way.

### bfsm.py
Module bfsm.py defines a class to implement a minimalistic finite state machine. It is given a matrix specifying the action for each {state,stimulus] combination, it contains an additional high priority queue to augment the last stimulus and optionally an action when entering a state can be defined.

### StoppableThread.py
Module StoppableThread.py defines a class derived from class threading.Thread which extends it with two options regarding stopping:
 A) Event _stop_event is added to the thread as well as methods to set and check this event. The thread itself has to check regularly for the stopped() condition.
 B) A call-back can be specified which is invoked at normal and abnormal termination of the thread. This is incorporated to prevent the main thread controller from polling the status of the thread(s) regularly.
The main loop is normally put in method run(), but this method is already defined in this class and should not be redefined in a derived class. In stead a derived class should define a method named loop() containing the main loop.

### watchdog.py
Module watchdog.py define a class for a simple watchdog timer. A timer can be started, stopped and restarted.

## Test enviroment
Script mqtt.rfxcom.py is tested (and in use) on a Raspberry Pi 3B running Raspbian 10 (buster). The MQTT broker is also running on this Raspberry Pi. An RFXcom RFXtrx433XL is in use as a transceiver, which is attached to a USB port of the aforementioned Raspberry Pi. Script mqtt.rfxcom.py uses about 40 CPU seconds per day in this environment.

