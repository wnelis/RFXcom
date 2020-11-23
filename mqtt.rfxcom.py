#!/usr/bin/python3
#
# Script mqtt.rfxcom.py allows for multiple programs to access simultaneously
# and independently an RFXcom RFXtrx433XL transceiver. As intermediate
# communications channel MQTT messages are used. This script subscribes to topic
# RFXcom/command, sends the received commands to the transceiver and publishes
# the responses via topic RFXcom/response. Unsolicited messages are published
# via topic RFXcom/message.
#
# One or more scripts may publish their commands on topic RFCxom/command and
# will receive the responses via topic RFXcom/response. These scripts should
# send messages using the following format:
#   <SourceName>;<RfxcomMessage>
# in which <SourceName> is a short and unique identifier of the script. The
# identifier should not contain a semicolon. <RfxcomMessage> is a complete
# message for the transceiver, however without the initial byte containing the
# length. The <SourceName>, followed by a semicolon, will be prepended to the
# associated response. The scripts should discard, without any (error) message,
# those responses starting with a <SourceName> which does not match their name.
#
# (Note: This script moves packets, basically without altering them, between two
#  media. In OSI terms this script is a bridge, operating at layer 2 of the OSI
#  model. Each packet to or from the RFXcom transceiver starts with a byte
#  specifying the length of the rest of the packet. The length field is
#  considered to be the OSI L2 header.)
#
# Reminders/to do:
# - Use another layer in the topic hierarchy for addressing the sources. A
#   source named 'ASource' sends a command using topic "rfxcom/command/ASource'
#   and expects the response on topic 'rfxcom/response/ASource'.
# - A method is needed to assure that each source has a unique name.
# - The reset command sent via topic 'rfxcom/command(/#)' is silently ignored.
# - The last mode command for each source should be remembered. Upon setting the
#   mode by any source, the combined mode should be sent to the transceiver.
#   This feature will only work in a rather static environment, as there is no
#   way for a source to tell (via MQTT) that it is stopping. Thus adding a
#   source and mode is simple, removing them is difficult.
# - Content of other commands is not inspected / checked.
# - Add thread Reporter, which reports the statistics to MQTT. It requires
#   script submqttm.py to accept a wildcard, as the number of sources is not
#   known a priori.
# - It is tempting to define own codes for a negative NACK.
#
# Written by W.J.M. Nelis, wim.nelis@ziggo.nl, 2020.10
#
import bfsm				# Basic finite state machine
import paho.mqtt.client as mqtt		# MQTT publisher / subscriber
import queue				# Inter thread communication
import re				# Intelligent string parsing
import serial				# Access to RFXcom transceiver via USB
import signal
import StoppableThread			# Thread class with stop options
import subprocess			# Invoke a shell command
import threading			# Running programs simultaneously
import time
import watchdog				# Watchdog timer


#
# Installation constants.
# =======================
#
__version__     = 0.10
_debug_submqttm_= True			# Set debug mode
LfDebug= '/home/pi/rfxcom/debug.log'	# Debug log file name

#
# MQTT related installation constants.
#
MqtBroker= '127.0.0.1'			# Network address of MQTT broker
MqtClient= 'rfxcom'			# Name of MQTT client
MqtTpcCmd= 'rfxcom/command'
MqtTpcRsp= 'rfxcom/response'
MqtTpcMsg= 'rfxcom/message'
#
MqtTpcSta= 'rfxcom/stats/'		# Head of statistics hierarchie

#
# FSM related installation constants.
#
FsmStiCmd= 'Cmd'			# Stimulus 'command received'
FsmStiRsp= 'Rsp'			# Stimulus 'reponse received'
FsmStiTim= 'Tmt'			# Stimulus 'Time out'

#
# RFXcom related installation constants.
#
RfxUsbCm= 'lsusb'			# Command to retrieve connected USB devices
RfxUsbId= b' 0403:6015 '		# USB identifier of RFXtrx433XL
RfxDevCm= ['ls','-l','/dev/serial/by-id/']  # Command to retrieve device name
RfxDevRe= 'usb\-(.+?)_(.+?)_(.+?)\-.+(ttyUSB\d)'  # Get info and device name
RfxDevRe= re.compile( RfxDevRe )	# One time compilation of expression
RfxSerSp= 38400				# Serial port bit rate [b/s]
RfxSerFT= 2				# Failure threshold
#
RfxMode = b'\x00\x00\x06\x00'		# Default bitmask to enable protocols
#
RfxCmdReset    = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
RfxCmdGetStatus= b'\x00\x00\x01\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00'
RfxRspGetStatus= b'\x01\x00\x01\x02'	# Response header
RfxCmdSetMode  = b'\x00\x00\x02\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00'
RfxRspSetMode  = b'\x01\x00\x02\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00'
RfxCmdStartRcvr= b'\x00\x00\x03\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00'
RfxRspStartRcvr= b'\x01\x07\x03\x07'	# Response header
RfxRspNegAck   = b'\x02\x01\x00\x02'	# Transceiver NAK response

#
# Global variables.
# =================
#
dlfo= None				# Debug log file object
#
# Mqtt related global variables.
# Queues between threads mqtt and dispatcher.
#
MqtQueCmd= queue.Queue()		# Commands to RFXcom
MqtQueRsp= queue.Queue()		# Responses from RFXcom
MqtQueMsg= queue.Queue()		# Unsolicited responses from RFXcom
#
# RFXcom related global variables.
# Queues between threads rfxcom and dispatcher.
#
RfxQueCmd= queue.Queue()		# Commands to RFXcom
RfxQueRsp= queue.Queue()		# (Unsolicited) responses from RFXcom


#
# Class definitions.
# ==================
#
# Class BaseThread extends class StoppableThread with method LogMessage.
#
class BaseThread( StoppableThread.StoppableThread ):

 #
 # Method LogMessage sends a message to the local syslog server.
 #
  if _debug_submqttm_:
    def LogMessage( self, Msg ):
      tom= time.strftime( '%Y%m%d %H%M%S' )
      dlfo.write( ' '.join( (tom,self.name,Msg,'\n') ) )
      dlfo.flush()
  else:
    def LogMessage( self, Msg ):
      syslog.openlog( 'MqttRfx', 0, syslog.LOG_LOCAL6 )
      syslog.syslog ( ' '.join( (self.name,Msg) ) )
      syslog.closelog()


#
# Define a class for each thread. As only one instance of each class will be
# created, there is no need for thread-local data (or to be more precise, there
# is no need for both thread-local data and instance-local data).
#

#
# Class Mqtt.
# -----------
#
# Class Mqtt implements an interface between the MQTT broker and a set of
# queues. It does not alter the content of the messages.
#
class Mqtt( BaseThread ):
  '''Interface between dispatcher and the MQTT broker.'''

  def __init__( self, *args, **kwargs ):
    super().__init__( *args, **kwargs )
    self.client= None
    self.state = 'init'
    self.thread= None

 #
 # Call-back _on_connect is invoked when attempting to create a connection to
 # the MQTT broker. In case of a recoverable error, a rebuild is attempted. In
 # case of an irrecoverable error, the script is stopped.
 #
  def _on_connect( self, client, userdata, flags, rc ):
    # rc: 0=Ok, 3= recoverable error, other=not recoverable error
    if   rc == 0:
      self.LogMessage( 'Connected to broker' )
    elif rc == 3:
      self.LogMessage( 'Connection to broker failed' )
      MqtQueMsg.put( None )		# Wake up child thread
      self.state= 'restart'		# Try again
    else:
      self.LogMessage( 'Connection to broker failed, fatal error' )
      self.stop()			# Irrecoverable error

 #
 # Call-back _on_disconnect is invoked upon termination of the connection to the
 # MQTT broker. In case of an unexpected disconnect, a rebuild of the connection
 # is attempted.
 #
  def _on_disconnect( self, client, userdata, rc ):
    # rc: 0=due to disconnect(), 1=due to other reason
    if rc == 0:
      self.LogMessage( 'Disconnected from broker' )
    else:
      self.LogMessage( 'Unexpected disconnect from broker' )
      MqtQueMsg.put( None )		# Wake up child thread
      self.state= 'restart'		# Try to recover

 #
 # Call-back _on_message moves any received message, which must be an RFXcom
 # transceiver command, immediately to an intermediate queue. Thread dispatch
 # will read those messages in it's own pace.
 #
 # Note: type(message.payload) is bytes == True
 #
  def _on_message( self, client, userdata, message ):
    # message is a class with members topic, payload, qos and retain.
#   now= time.time()			# Time of receipt of message
#   MqtQueCmd.put( (message.payload,now) )
    MqtQueCmd.put( message.payload )
    self.LogMessage( '  cmd <{}>'.format(message.payload) )  # TEST

 #
 # Private method _connect builds an MQTT client object, creates a connection to
 # the MQTT broker and subscribes to the topic with RFXcom commands. Using
 # loop_start, an additional thread is started to handle the communications with
 # the broker.
 #
  def _connect( self ):
  # Create mqtt client object and connect to broker
    self.client= mqtt.Client( MqtClient )
    self.client.on_connect   = self._on_connect
    self.client.on_disconnect= self._on_disconnect
    self.client.on_message   = self._on_message
    self.client.connect( MqtBroker )
  # Subscribe to RFXcom command channel
    self.client.subscribe( (MqtTpcCmd,0) )
    self.client.loop_start()

 #
 # Private method _disconnect reverses the actions of method _connect.
 #
  def _disconnect( self ):
    if self.client is not None:
      self.client.loop_stop()
      self.client.disconnect()
      self.client= None

 #
 # Method write_rsp waits for a response on a submitted command and publishes it
 # via the MQTT broker.
 #
  def write_rsp( self ):
    rsp= MqtQueRsp.get()
    if rsp is None:  return
    self.client.publish( MqtTpcRsp, rsp )
    self.LogMessage( '  rsp <{}>'.format(rsp) )  # TEST

 #
 # Method write_msg is started as a thread. It takes the unsolicited messages
 # received from the RFXcom transceiver and publishes them on the MQTT broker.
 #
  def write_msg( self ):
    while not self.stopped():
      msg= MqtQueMsg.get()		# Wait for unsolicited message
      if msg is None:  break		# Stop if state has changed
      self.client.publish( MqtTpcMsg, msg )
      self.LogMessage( '  msg <{}>'.format(msg) )  # TEST

 #
 # Method loop is a very small fsm, which initiates or restarts the connection
 # to the MQTT broker, and passes responses from the RFXcom transceiver when in
 # the operational state.
 #
  def loop( self ):
    self.LogMessage( 'Starting thread' )
  #
    while not self.stopped():
      if   self.state == 'init':
        self._connect()			# Connect and subscribe
        self.thread= threading.Thread( target=self.write_msg )
        self.thread.start()
        self.state = 'run'
      elif self.state == 'run':
        self.write_rsp()
      elif self.state == 'restart':
        if self.thread is not None:
          self.thread.join()
          self.thread= None
        self._disconnect()
        self.state = 'init'
  #
    self.LogMessage( 'Stopping thread' )
    self._disconnect()

 #
 # Method stop stops this thread. Dummy messages are put in queues to unblock
 # Queue.get method invocations.
 #
  def stop( self ):
    super().stop()			# Request to stop this thread
    MqtQueRsp.put( None )		# Unblock method write_rsp
    MqtQueMsg.put( None )		# Unblock method write_msg


#
# Class Dispatcher.
# -----------------
#
# Class Dispatcher moves the commands, responses and messages (unsolicited
# responses) between the various queues, while taking care of the addressing and
# the OSI L2 encapsulation.
# It implements a flow control mechanism in the direction of the RFXcom
# transceiver with a window size of 1. The source identifier and the sequence
# number of the command are prepended respectively inserted in the associated
# response. The multiplexed stream of commands is assigned a new sequence
# number, which has only significance between this thread and the RFXcom
# transceiver.
#
class Dispatcher( BaseThread ):

 #
 # Define the call-back functions.
 #
  def on_timeout( self ):
    self.dfsm.HandleEvent( FsmStiTim )

 #
 # Define the actions for the finite state machine.
 #
  def do_nothing( self ):
    pass				# Do nothing

 #
 # Action do_error is invoked for a pair (State,Stimulus) which should not
 # occur.
 #
  def do_error( self, arg=None ):
    self.LogMessage( 'FSM error, state={}, stim={}, arg=<{}>'.format(
                     self.dfsm.State, self.dfsm.Stimulus, arg ) )

 #
 # Method do_inc_seq increments a sequence number.
 #
  def do_inc_seq( self, value ):
    return value + 1  if value < 255 else 1

 #
 # Method do_syntax checks the syntax of an incoming command. It should start
 # with a source identifier, folllowed by ';', followed by a command to be sent
 # to the RFXcom transceiver. The function returns True iff the syntax is
 # correct.
 #
  def do_syntax( self, extcmd ):
    try:
      (srcid,rawcmd)= extcmd.split( b';', maxsplit=1 )
    except ValueError:			# Probably no delimiter in command
      return False			# Wrong syntax
    if len(srcid)  == 0:  return False	# Illegal source identifier
    srcid= srcid.strip()		# Remove surrounding whitespace
    if len(srcid)  == 0:  return False	# Illegal source identifier
    if len(rawcmd) == 0:  return False	# Illegal command
    if len(rawcmd) > 127: return False	# Illegal command
  #
    if type(rawcmd) is bytes:
      rawcmd= bytearray( rawcmd )	# Make mutable list of bytes
    self.cmdisn= rawcmd[2]		# Extract sequence number
    rawcmd[2]= 0			# Reset sequence number
    self.cmdraw= rawcmd
    self.cmdsrc= srcid
    if srcid not in self.cmdsts:
      self.cmdsts[srcid]= { 'packet':0, 'octet':0 }
  #
    return True

 #
 # Action do_cmd handles a command received via MQTT. The source identifier is
 # stripped from the command, the sequence number is set, and the raw command is
 # forwarded to the next stage, that is thread Rfxcom.
 #
  def do_cmd( self, cmd ):
    if self.do_syntax( cmd ):
      self.cmdsts[self.cmdsrc]['packet']+= 1	# Update source statistics
      self.cmdsts[self.cmdsrc]['octet'] += 1 + len(self.cmdraw)
  #
      self.cmdraw[2]= self.cmdesn	# Set egress sequence number
      self.cmdesn   = self.do_inc_seq( self.cmdesn )
      self.cmdwdt.reset()		# start watchdog timer
  #
      RfxQueCmd.put( self.cmdraw )	# Send cmd to RFXcom
    else:
      self.LogMessage( 'Illegal syntax: <{}>'.format(cmd) )

 #
 # Action do_msg handles the receipt of an unsolicited response a.k.a. message.
 #
  def do_msg( self, msg ):
    if msg[0] < 0x03:
      self.LogMessage( 'Illegal unsolicited response: <{}>'.format(msg) )
      return
#   msg[2]= 0				 # Clear sequence number
    self.msgsts['packet']+= 1
    self.msgsts['octet' ]+= 1 + len(msg)
    MqtQueMsg.put( msg )
    return

 #
 # Action do_rsp handles a response, which might be unsolicited, from the
 # RFXcom transceiver.
 #
  def do_rsp( self, rsp ):
    if rsp[0] > 0x02:			# Check for an unsolicited message
      self.do_msg( rsp )
      return
  #
    if rsp[2] == self.rspisn:
      self.cmdwdt.stop()		# Stop the watchdog timer
      self.rspisn= self.do_inc_seq( self.rspisn )
      rsp[2]= self.cmdisn		# Restore original sequence number
      rsp= self.cmdsrc + b';' + rsp
      MqtQueRsp.put( rsp )		# Move to thread Mqtt
      self.feednc.set()			# Allow for next command
    else:
      self.LogMessage( 'Unexpected sequence number: <{}>'.format(rsp) )

  def do_timeout( self ):
    rsp= bytearray(RfxRspNegAck)	# Negative acknowledge response
    rsp[2]= self.cmdisn			# Set sequence number
    rsp= self.cmdsrc + b';' + rsp
    MqtQueRsp.put( rsp )		# Move to thread Mqtt
    self.feednc.set()			# Allow for next command

  def __init__( self, *args, **kwargs ):
    super().__init__( *args, **kwargs )

    self.fsmtm= {			# Finite state machine transition matrix
      'Init' : {
        FsmStiCmd : ( 'Wait', self.do_cmd     ),
        FsmStiRsp : ( 'Init', self.do_msg     ),
        FsmStiTim : ( 'Init', self.do_nothing ),
      },
      'Wait' : {
        FsmStiCmd : ( 'Wait', self.do_error   ),
        FsmStiRsp : ( 'Init', self.do_rsp     ),
        FsmStiTim : ( 'Init', self.do_timeout ),
      }
    }

    self.dfsm  = bfsm.Bfsm( self.fsmtm )# Create finite state machine
    self.feeder= None			# Command feeder thread
    self.feednc= threading.Event()	# Set if next command allowed
    self.feednc.set()			# Next command can be sent
    self.cmdraw= None			# Current command
    self.cmdisn= 0			# Current command: ingress sequence number
    self.cmdesn= 1			# Current command: egress sequence number
    self.cmdsrc= None			# Current command: source id
    self.cmdwdt= watchdog.WatchdogTimer( 3.5, self.on_timeout )
    self.rspisn= 1			# Next response: sequence number
    self.cmdsts= {}			# Statistics per source
    self.msgsts= { 'packet':0, 'octet':0 }	# Statistics of the unsolicited responses

 #
 # Method cmd_feeder is started as a thread. If the state allows, that is if
 # event self.feednc is set, it gets the next command from queue MqtQueCmd and
 # passes it, with the appropriate stimulus, to the FSM.
 #
  def cmd_feeder( self ):
    while not self.stopped():
      self.feednc.wait()		# Wait for clearance
      self.feednc.clear()		# Remove clearance
      if self.stopped():  return	# Exit if script needs to stop
      cmd= MqtQueCmd.get()		# Fetch next command
      if cmd is None:  return		# Exit if script needs to stop
      self.dfsm.HandleEvent( FsmStiCmd, cmd )

  def rsp_feeder( self ):
    rsp= RfxQueRsp.get()		# Fetch next response
    if rsp is None:  return		# Exit is script needs to stop
    self.dfsm.HandleEvent( FsmStiRsp, rsp )

  def loop( self ):
    self.LogMessage( 'Starting thread' )
    self.feeder= threading.Thread( target=self.cmd_feeder )
    self.feeder.start()
  #
    while not self.stopped():
      self.rsp_feeder()
  #
    self.feeder.join()
    self.LogMessage( 'Stopping thread' )

  def stop( self ):
    super().stop()
    self.feednc.set()
    MqtQueCmd.put( None )
    RfxQueRsp.put( None )


#
# Class Rfxcom.
# -------------
#
# Class Rfxcom implements an interface to the RFXcom transceiver. This class
# only handles the OSI L2 transformations, that is the length byte is appended
# on write and is checked and stripped on read.
#
class Rfxcom( BaseThread ):

  def __init__( self, *args, **kwargs ):
    super().__init__( *args, **kwargs )
    self.model = None			# RFXcom device model name
    self.sn    = None			# RFXcom device serial number
    self.hardwr= ''			# RFXcom hardware version number
    self.firmwr= ''			# RFXcom firmware version number
    self.port  = None			# Serial port name
    self.serial= None			# Serial port object instance
    self.thread= None			# Thread reading from serial port
    self.state = 'init'			# Operational state of thread
    self.seqnbr= 0			# Packet sequence number
    self.fails = 0			# Count of successive failures
    self.stats = dict(
      usb_resets         = 0,		# USB connection reset count
      usb_egress_packets = 0,		# USB egress packet count
      usb_egress_octets  = 0,		# USB egress octet count
      usb_egress_errors  = 0,		# USB egress error count
      usb_ingress_packets= 0,		# USB ingress packet count
      usb_ingress_octets = 0,		# USB ingress octet count
      usb_ingress_errors = 0,		# USB ingress error count
    )


 #
 # Method _build_connection checks if the RFXcom transceiver is connected via
 # USB and if so, creates a serial connection to the transceiver. It returns a
 # True value if the connection is created, and a False value if for some reason
 # the connection attempt failed.
 #
  def _build_connection( self ):
    self.LogMessage( 'Build serial connection' )
    self.model= None
    self.sn   = None
    self.port = None
    if self.serial is not None:		# See if serial connection exists
      self.serial.close()
      self.serial= None

  # See if the RFXcom is connected to a USB port.
    cp= subprocess.check_output( RfxUsbCm )
    if cp.find( RfxUsbId ) == -1:
      self.LogMessage( 'RFXcom transceiver not connected' )
      return False
  # Retrieve the name of the associated serial device.
    cp= subprocess.check_output( RfxDevCm ).decode( 'utf-8' )
    for line in cp.split( '\n' ):
      mo= RfxDevRe.search( line )
      if mo is not None:
        if mo.group(1) == 'RFXCOM':
          self.model= mo.group(2)
          self.sn   = mo.group(3)
          self.port = '/dev/' + mo.group(4)
    if self.port is None:
      self.LogMessage( 'RFXcom transceiver not found' )
      return False
  # Create a serial connection to the RFXcom transceiver
    try:
      self.serial= serial.Serial( self.port, RfxSerSp, timeout=1 )
    except FileNotFoundError:
      self.LogMessage( 'Could not connect to RFXcom transceiver' )
      self.serial= None
      return False
  #
    self.serial.write_timeout= 0.1	# Write time out [s]
    return True				# Serial connection is created

 #
 # Method _reset_rfxcom resets the RFXcom transceiver and prepares it for normal
 # operation. It is assumed that the serial connection already is operational
 # and that thread read is not active.
 #
  def _reset_rfxcom( self ):
    self.LogMessage( 'Reset RFXcom transceiver' )
  # Issue the reset command and wait for it to complete.
    self.stats['usb_resets']+= 1	# Update statistics
    if not self._write( RfxCmdReset )    :  return False	# No response
    time.sleep( 0.1 )
  # Start the transceiver by sending the 'get status' command.
    self.serial.reset_input_buffer()
    self.serial.reset_output_buffer()
    if not self._write( RfxCmdGetStatus ):  return False
  # Read the status response.
    bfr= self._read( 0.5 )
    if len(bfr) != 20:  return False
  # Interpret the received response.
    if bfr[0:4] != RfxRspGetStatus:  return False
    self.hardwr= '{:d}.{:03d}'.format(bfr[10],bfr[11])	# msg7, msg8
    self.firmwr= '{:02x}.{:03d}'.format(bfr[13],bfr[5])	# msg10, msg2
  # Set the set of enabled protocols to the default set.
    if bfr[6:10] != RfxMode:
      self.LogMessage( 'Activate default set of protocol decoders')
      bfr= RfxCmdSetMode		# Generic set-mode command
      bfr[6:10]= RfxMode		# Set default protocol selection
      if not self._write( bfr ):  return False
      bfr= self._read( 0.5 )		# Fetch response
      if len(bfr) != 20:  return False
  # Interpret the received response
      if bfr[0:4]  != RfxRspSetMode[0:4]:  return False
      if bfr[6:10] != RfxMode           :  return False
  # Send the StartReceiver command and read the response.
    if not self._write( RfxCmdStartRcvr ):  return False
    bfr= self._read( 0.5 )
    if len(bfr) != 20:  return False
  # Interpret the received response.
    if bfr[0:4] != RfxRspStartRcvr:  return False
    if bfr[4:20].decode('utf8') != 'Copyright RFXCOM':  return False
    return True

 #
 # Method _read reads one packet from the RFXcom transceiver. The packet should
 # arrive within the specified time. The received packet is returned, but in
 # case of a timeout, a packet with a zero length or other error, an empty
 # string is returned.
 #
  def _read( self, TimeOut ):
  # Read the length of the packet, using the supplied timeout.
    self.serial.timeout= TimeOut	# Set read time out
    bfr= b''				# Clear packet buffer
    lng= self.serial.read( 1 )		# Fetch length of next packet
    if self.stopped():			# If thread needs to stop
      return bfr			#   return an error indicator
  # If a valid length is read, read the packet too.
    if lng == b'':			# A time out results in an empty string
      lng= 0				# Change type to integer
    else:
      lng= int.from_bytes( lng, byteorder='big' )
      if lng == 0:
        self.fails+= 1
      else:
        self.serial.timeout= 0.1	# Rest of packet should arrive quickly
        bfr= self.serial.read( lng )
  # Update statistics.
      self.stats['usb_ingress_packets']+= 1
      self.stats['usb_ingress_octets' ]+= len(bfr) + 1
  #
    if   lng == 0:			# Most probably a time-out
      return bfr
    elif lng == len(bfr):		# If all seems ok,
      self.fails= 0			#   reset failure counter
      return bytearray(bfr)
    else:
      self.stats['usb_ingress_errors']+= 1
      self.fails+= 1
      return b''

 #
 # Method _write writes one packet to the RFXcom transceiver. It returns a True
 # value if the write succeeded.
 #
  def _write( self, packet ):
    assert self.serial is not None, 'No serial connection'
    assert len(packet) < 255, 'Packet is too large'
  # Update the statistics.
    self.stats['usb_egress_packets']+= 1
    self.stats['usb_egress_octets' ]+= len(packet) + 1
  # Send the lengh of the packet
    try:
      self.serial.write( bytes([len(packet)]) )
    except:
      self.stats['usb_egress_errors']+= 1
      return False
  # Send the packet itself
    try:
      self.serial.write( packet )
    except:
      self.stats['usb_egress_errors']+= 1
      return False
  #
    return True

 #
 # Method read is started as a separate thread. It reads packets from the serial
 # port, thus from the RFXcom transceiver. If a packet has been read it is
 # passed to the appropriate internal queue. If too many (fatal) errors are
 # encountered, then it will cause a rebuild of the communications channel.
 #
  def read( self ):
    while not self.stopped():
      bfr= self._read( 10 )
      if len(bfr) > 0:
        if _debug_submqttm_:
          self.LogMessage( 'rsp <{}>'.format(bfr) ) # TEST
        RfxQueRsp.put( bfr )
      if self.fails > RfxSerFT:
        RfxQueCmd.put( None )		# Inhibit blocking in method write
        self.state= 'restart'
        break
    return

 #
 # Method write wait for a packet containing either a mode command or a device
 # command. The packet is written to the RFXcom transceiver. If writing fails a
 # NAK response is sent back to the source.
 #
  def write( self ):
    bfr= RfxQueCmd.get()
    if bfr is None:  return		# Wake up call, state change(d)
    if _debug_submqttm_:
      self.LogMessage( 'cmd <{}>'.format(bfr) ) # TEST
    if bfr == RfxCmdReset:  return	# Silently ignore reset command
  #
    self.seqnbr= bfr[2]
    if self.state == 'run':
      if self._write( bfr ):  return
      self.fails+= 1
  # Send a NAK, with the correct sequence number
      bfr   = bytearray(RfxRspNegAck)
      bfr[2]= self.seqnbr
      RfxQueRsp.put( bfr )

 #
 # Method loop reads from and writes to the RFXcom transceiver. If there is a
 # problem with the serial connection to the transceiver, it will terminate and
 # rebuild the connection.
 #
  def loop( self ):
    self.LogMessage( 'Starting thread' )
    while not self.stopped():

  # State init: build serial connection to the RFXcom transceiver. User I/O is
  # not possible in this state.
      if self.state == 'init':
        if self._build_connection():	# Build serial connection
          if self._reset_rfxcom():	# Prepare RFXcom transceiver
            self.state = 'run'
            self.thread= threading.Thread( target=self.read )
            self.thread.start()
          else:
            time.sleep( 10 )
        else:
          time.sleep ( 10 )

  # State run: move user data to and from the RFXcom transceiver.
      elif self.state == 'run':
        self.write()

  # State restart: Some serious problems have been encountered on the
  # communications channel to the RFXcom transceiver. Wait for the reader thread
  # to stop, clean up and rebuild the communications channel.
      elif self.state == 'restart':
        self.thread.join()		# Wait for read thread to be finished
        self.thread= None
        self.serial.close()		# Terminate serial connection
        self.serial= None
        self.port  = None
        self.state = 'init'
        self.fails = 0
  #
    self.LogMessage( 'Stopping thread' )
# self.serial.close() ??

  def stop( self ):
    super().stop()
    RfxQueCmd.put( None )		# Terminate wait in method write


#
# Utilities called from the main program.
#
MainThread= threading.Event()		# Set to stop this script

#
# Function ReportAndStop writes the supplied message to the appropriate device
# and sets the flag to stop this script.
#
def ReportAndStop( msg ):
  if msg is not None:
    if _debug_submqttm_:
      dlfo.write( msg + '\n' )
      dlfo.flush()
    else:
      syslog.openlog( 'submqttm', 0, syslog.LOG_LOCAL6 )
      syslog.syslog ( msg )
      syslog.closelog()
  MainThread.set()			# Set flag to stop script

#
# Call-back HandleSignal is invoked whenever a SIGINT or SIGTERM signal arrives.
# It reports the receipt of the signal and sets the flag to stop this script.
#
def HandleSignal( signum, frame ):
  msg= 'Termination signal {} received'.format( signal.Signals(signum).name )
  ReportAndStop( msg )

#
# Call-back OnTerminate is invoked whenever a thread stops, either in a normal
# or in an abnormal (exception) way. In either case, the flag to stop this script
# is set.
#
def OnTerminate( Name, Except ):
  msg= None
  if Except is not None:
    msg= Name + ': ' + Except
  ReportAndStop( msg )

#
# Main program.
# =============
#
if _debug_submqttm_:
  dlfo= open( LfDebug, 'a' )

#
# Set up handling of termination signals.
#
signal.signal( signal.SIGINT , HandleSignal )
signal.signal( signal.SIGTERM, HandleSignal )

threads= []				# List of active threads
th0= Rfxcom    ( OnTerminate, name='rfxcom'   ) ;  threads.append(th0) ;  th0.start()
th1= Dispatcher( OnTerminate, name='dispatch' ) ;  threads.append(th1) ;  th1.start()
th2= Mqtt      ( OnTerminate, name='mqtt'     ) ;  threads.append(th2) ;  th2.start()
# th3= Reporter  ( OnTerminate, name='reporter' ) ;  threads.append(th3) ;  th3.start()

#
# Monitor the state of the threads of this script. If one thread dies or if
# event MainThread is set, all (other) threads, including this main thread,
# should stop (too).
#
while len(threads) > 0:
  try:
    MainThread.wait()
    for t in reversed( threads ):	# Note the order of this loop
      if t.is_alive():
        t.stop()
      t.join()
      threads.remove( t )		# Thread has stopped

  except KeyboardInterrupt:
    MainThread.set()			# Set flag to stop this script

if _debug_submqttm_:
  dlfo.close()
