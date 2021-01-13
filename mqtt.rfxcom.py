#!/usr/bin/python3
#
# Script mqtt.rfxcom.py allows for multiple programs to access simultaneously
# and independently an RFXcom RFXtrx433XL transceiver. As intermediate
# communications channel MQTT messages are used. This script subscribes to topic
# rfxcom/command/#, sends the received commands to the transceiver and publishes
# the responses via topic rfxcom/response/#. Unsolicited messages are published
# via topic rfxcom/message.
#
# A script may publish it's commands on topic rfxcom/command/<SourceName> and it
# will receive the responses via topic rfxcom/response/<SourceName>, in which
# <SourceName> is a short and unique identifier of the script. The command is a
# complete message for the transceiver, however without an initial byte
# containing the length of the rest of the message.
#
# Note: This script moves packets, basically without altering them, between two
#   media. In OSI terms this script is a bridge, operating at layer 2 of the OSI
#   model. Each packet to or from the RFXcom transceiver starts with a byte
#   specifying the length of the rest of the packet. The length field is
#   considered to be the OSI L2 header. Thus this header is added when sending
#   to the transceiver and removed when receiving from the transceiver.
#
# Reminders/to do:
# - A method is needed to assure that each source has a unique name.
# - The last mode command for each source should be remembered. Upon setting the
#   mode by any source, the combined mode should be sent to the transceiver.
#   This feature will only work in a rather static environment, as there is no
#   way for a source to tell (via MQTT) that it is stopping. Thus adding a
#   source and mode is simple, removing them is difficult.
# - It is tempting to define own codes for a negative NACK.
#
# Written by W.J.M. Nelis, wim.nelis@ziggo.nl, 2020.10
#
import bfsm				# Basic finite state machine
import json				# Encode structure to sent via MQTT
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
__version__     = 0.20
# In debug mode more messages are generated and those message are written to the
# file specified in constant LfDebug. If debug mode is disabled, less messages
# are generated and those messages are written to the local syslog server.
_debug_mqtt_rfx_= False			# Flag: debug mode
LfDebug= '/full/path/to/debug.log'	# Debug log file name
# Optionally, this script can push both status and statistics periodically using
# a fixed topic. A possible use of this feature is to address a Xymon client,
# which transforms those MQTT messages into HTML-encoded messages which are sent
# to a Xymon server.
_include_push_  = False			# Flag: push info periodically
_push_srcid_    = 'xymon'		# Pseudo source identifier
_push_timing_   = ( 300, 3 )		# Timing: repeat push every 300 [s]

#
# MQTT related installation constants.
#
MqtBroker= 'your.broker.address'	# Network address of MQTT broker
MqtClient= 'rfxcom'			# Name of MQTT client
MqtTpcCmd= 'rfxcom/command'		# Topic without source identifier
MqtTpcRsp= 'rfxcom/response'		# Topic without source identifier
MqtTpcMsg= 'rfxcom/message'
#
MqtTpcMuxCon= 'connected/rfxcom_bridge'
MqtTpcMuxPsa= 'rfxcom_bridge/status'		# Topic without source identifier
MqtTpcMuxPsi= 'rfxcom_bridge/statistics'	# Topic without source identifier
# MqtTpcMuxPco= 'rfxcom_bridge/config'		# Topic without source identifier
MqtTpcMuxGet= 'rfxcom_bridge/get'		# Common prefix of pull commands
MqtTpcMuxGsu= 'rfxcom_bridge/get/status'	# Topic without source identifier
MqtTpcMuxGsi= 'rfxcom_bridge/get/statistics'	# Topic without source identifier
# MqtTpcMuxGco= 'rfxcom_bridge/get/config'	# Topic without source identifier

#
# Finite state machine (fsm) related installation constants.
#
FsmStiCmd= 'Cmd'			# Stimulus 'command received'
FsmStiRsp= 'Rsp'			# Stimulus 'reponse received'
FsmStiMsg= 'Msg'			# Stimulus 'message received'
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
# Queues between threads Mqtt and Dispatcher.
#
MqtQueCmd= queue.Queue()		# Commands to RFXcom
MqtQueRsp= queue.Queue()		# Responses from RFXcom
MqtQueMsg= queue.Queue()		# Unsolicited responses from RFXcom
#
# RFXcom related global variables.
# Queues between threads Rfxcom and Dispatcher.
#
RfxQueCmd= queue.Queue()		# Commands to RFXcom
RfxQueRsp= queue.Queue()		# (Unsolicited) responses from RFXcom
#
# Control plane related global variables.
# Queues between threads Mqtt and Controller.
#
CtlQueIng= queue.Queue()		# Ingress, messages to Controller
CtlQueEgr= queue.Queue()		# Egress, messages from Controller


#
# Class definitions.
# ==================
#
# Abstract class BaseThread extends class StoppableThread with method
# LogMessage.
#
class BaseThread( StoppableThread.StoppableThread ):

 #
 # Method LogMessage sends a message to the local syslog server.
 #
  if _debug_mqtt_rfx_:
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
# Abstract class ControlThread extends class BaseThread with two methods which
# collect and send either the status or the statistics of this bridge.
#
class ControlThread( BaseThread ):
  global threads

  def __init__( self, *args, **kwargs ):
    super().__init__( *args, **kwargs )
    self.bridge= dict(
      bridge_version = __version__,	# Script version number
      bridge_uptime  = time.time()	# UTS of start of script
    )
    self.rfxcom= None			# Ref to thread Rfxcom
    self.dsptch= None			# Ref to thread Dispatcher
    self.mqtt  = None			# Ref to thread Mqtt
    time.sleep( 1 )			# Wait for all threads to be created
    for t in threads:
      if   t.name == 'rfxcom':
        self.rfxcom= t
      elif t.name == 'dispatch':
        self.dsptch= t
      elif t.name == 'mqtt':
        self.mqtt  = t
    assert self.rfxcom is not None
    assert self.dsptch is not None
    assert self.mqtt   is not None

 #
 # Private method _send_status collects the status information and forwards it
 # as a JSON-encoded message to thread Mqtt for transmission.
 #
  def _send_status( self, src, uts ):
    stats= {}				# Create empty result area
    stats['Device']= self.rfxcom.device	# RFXcom device information
    stats['Connection']= 'On' if self.rfxcom.state == 'run' else 'Off'
    CtlQueEgr.put( (MqtTpcMuxPsa+'/'+src,json.dumps(stats),uts) )

 #
 # Private method _send_statistics collects the statistics and forwards them as
 # a JSON-encoded message to thread Mqtt for transmission.
 #
  def _send_statistics( self, src, uts ):
    stats= {}				# Create empty result area
    stats['Bridge']= self.bridge	# Bridge parameters
    stats['USB'   ]= self.rfxcom.stats	# RFXcom throughput figures
    stats['Msg'   ]= self.dsptch.msgsts	# RFXcom message statistics
    stats['Cmd'   ]= self.dsptch.cmdsts	# RFXcom cmd/rsp per source
    stats['Error' ]= self.dsptch.errsts	# Errors detected by dispatcher
    stats['Mqtt'  ]= self.mqtt.stats	# Mqtt throughput figures
    CtlQueEgr.put( (MqtTpcMuxPsi+'/'+src,json.dumps(stats),uts) )


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
    self.client= None			# MQTT client object
    self.state = 'init'
    self.thrrfx= None			# Thread to move RFXcom messages
    self.thrctl= None			# Thread to move controller push messages
    self.stats = dict(
      mqtt_resets = 0,			# MQTT connection reset count
      topic_errors= 0,			# Unrecognised topics
      cmd_errors  = 0,			# Syntax errors in commands
      ingress_packets     = 0,		# Ingress packet count
      ingress_total_octets= 0,		# Ingress octet count
      ingress_data_octets = 0,		# Ingress payload octet count
      egress_packets      = 0,		# Egress packet count
      egress_total_octets = 0,		# Egress octet count
      egress_data_octets  = 0,		# Egress payload octet count
    )

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
      self.state= 'restart'		# Try again
      MqtQueMsg.put( None )		# Wake up child threads
      MqtQueRsp.put( None )
      CtlQueEgr.put( None )
      self.stats['mqtt_resets']+= 1	# Update statistics
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
      self.client= None			# Prevent another disconnect
      self.state= 'restart'		# Try to recover
      MqtQueMsg.put( None )		# Wake up child threads
      MqtQueRsp.put( None )
      CtlQueEgr.put( None )
      self.stats['mqtt_resets']+= 1	# Update statistics

 #
 # Call-back _on_message moves any received message, which must be either an
 # RFXcom transceiver command or a get-command to the control plane, immediately
 # to an intermediate queue. The receivers will read those messages in their own
 # pace.
 #
  def _on_message( self, client, userdata, message ):
    now= time.time()			# Time of receipt of message
    pld= message.payload
    tpc= message.topic
    self.stats['ingress_packets']     += 1
    self.stats['ingress_data_octets'] += len(pld)
    self.stats['ingress_total_octets']+= len(pld) + len(tpc.encode('utf8'))

    try:
      (tpc,src)= tpc.rsplit( '/', maxsplit=1 )
    except:
      self.LogMessage( 'Syntax error in topic: <{}>'.format(message.topic) )
      self.stats['topic_errors']+= 1
      return
    src= src.strip()			# Clean up source identifier
    if len(src) == 0:			# A source must have been specified
      self.LogMessage( 'Source not found: <{}>'.format(message.topic) )
      self.stats['topic_errors']+= 1
      return

    if   tpc == MqtTpcCmd:		# Check for RFXcom command
      if type(pld) is str  :  pld= bytearray( pld, 'utf-8' )
      if type(pld) is bytes:  pld= bytearray( pld )
      MqtQueCmd.put( (src,pld,now) )
      if _debug_mqtt_rfx_:
        self.LogMessage( '  cmd {}:<{}>'.format(src,pld) )
    elif tpc.find(MqtTpcMuxGet) == 0:	# Check for bridge get command
      CtlQueIng.put( (tpc,src,pld,now) )
      if _debug_mqtt_rfx_:
        self.LogMessage( '  ctl {}/{}:<{}>'.format(tpc,src,pld) )
    else:
      self.LogMessage( 'Topic not recognised: <{}/{}>'.format(tpc,src) )
      self.stats['topic_errors']+= 1

 #
 # Private method _connect builds an MQTT client object, creates a connection to
 # the MQTT broker and subscribes to the topics with RFXcom commands and with
 # the bridge get commands. Using loop_start, an additional thread is started to
 # handle the communications with the broker.
 #
  def _connect( self ):
  # Create mqtt client object
    self.client= mqtt.Client( MqtClient )
    self.client.on_connect   = self._on_connect
    self.client.on_disconnect= self._on_disconnect
    self.client.on_message   = self._on_message
  # Set last will and connect to broker
    self.client.will_set( MqtTpcMuxCon, 'Off', retain=True )
    self.client.connect( MqtBroker )
  # Subscribe to RFXcom command topics and bridge get-request topics
    self.client.subscribe( [ (MqtTpcCmd+'/#',0), (MqtTpcMuxGet+'/#',0) ] )
    self.client.loop_start()
    self.client.publish( MqtTpcMuxCon, 'On', retain=True )

 #
 # Private method _disconnect reverses the actions of method _connect.
 #
  def _disconnect( self ):
    if self.client is not None:
      self.client.publish( MqtTpcMuxCon, 'Off', retain=True )
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
    assert type(rsp) is tuple, 'Wrong type of queueing vehicle'
    assert len(rsp) == 2, 'Incorrect length of queueing vehicle'
    (src,pld)= rsp			# Unwrap tuple
    tpc= MqtTpcRsp + '/' + src		# Build topic
    self.client.publish( tpc, pld )
    self.stats['egress_packets']     += 1
    self.stats['egress_data_octets'] += len(pld)
    self.stats['egress_total_octets']+= len(pld) + len(tpc.encode('utf8'))
    if _debug_mqtt_rfx_:
      self.LogMessage( '  rsp {}:<{}>'.format(src,pld) )

 #
 # Method write_msg is started as a thread. It takes the unsolicited messages
 # received from the RFXcom transceiver and publishes them on the MQTT broker.
 #
  def write_msg( self ):
    while not self.stopped():
      msg= MqtQueMsg.get()		# Wait for unsolicited message
      if msg is None:  break		# Stop if state has changed
      assert type(msg) is bytearray, 'Wrong type of queueing vehicle'
      self.client.publish( MqtTpcMsg, msg )
      self.stats['egress_packets']     += 1
      self.stats['egress_data_octets'] += len(msg)
      self.stats['egress_total_octets']+= len(msg) + len(MqtTpcMsg.encode('utf8'))
      if _debug_mqtt_rfx_:
        self.LogMessage( '  msg <{}>'.format(msg) )

 #
 # Method write_psh is started as a thread. It takes the pushed statistics,
 # status and configuration reports received from thread Controller and
 # publishes them on the MQTT broker.
 #
  def write_psh( self ):
    while not self.stopped():
      msg= CtlQueEgr.get()		# Wait for pushed message
      if msg is None:  break		# Stop if state has changed
      assert type(msg) is tuple, 'Queued item has wrong type'
      assert len(msg) == 3, 'Queued tuple has wrong length'
      (tpc,msg,uts)= msg		# Unwrap tuple
      self.client.publish( tpc, msg )
      self.stats['egress_packets']     += 1
      self.stats['egress_data_octets'] += len(msg)
      self.stats['egress_total_octets']+= len(msg) + len(tpc.encode('utf8'))
      if _debug_mqtt_rfx_:
        if not _include_push_  or  not tpc.endswith( '/'+_push_srcid_ ):
          self.LogMessage( '  psh {}:<{}>'.format(tpc,msg) )

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
        self.thrrfx= threading.Thread( target=self.write_msg )
        self.thrrfx.start()
        self.thrctl= threading.Thread( target=self.write_psh )
        self.thrctl.start()
        self.state = 'run'
      elif self.state == 'run':
        self.write_rsp()
      elif self.state == 'restart':
        if self.thrrfx is not None:
          self.thrrfx.join()
          self.thrrfx= None
        if self.thrctl is not None:
          self.thrctl.join()
          self.thrctl= None
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
    CtlQueEgr.put( None )		# Unblock method write_psh


#
# Class Dispatcher.
# -----------------
#
# Class Dispatcher moves the commands, responses and messages (unsolicited
# responses) between the various queues, while taking care of the addressing and
# the OSI L2 encapsulation.
# It also implements a flow control mechanism in the direction of the RFXcom
# transceiver with a window size of 1. The source identifier and the sequence
# number of the command are prepended respectively inserted in the associated
# response. The multiplexed stream of commands is assigned a new sequence
# number, which has only significance between this thread and the RFXcom
# transceiver.
#
class Dispatcher( BaseThread ):
  '''Move commands and responses between an RFXcom transceiver and an MQTT broker.'''

 #
 # Call-back on_timeout is invoked if the timer, started in the fsm, expires.
 # Send a timeout stimulus to the fsm.
 #
  def on_timeout( self ):
    self.dfsm.HandleEvent( FsmStiTim )
    self.errsts['time_outs']+= 1	# Update error statistics

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
    self.errsts['fsm_errors']+= 1	# Update error statistics

 #
 # Method do_inc_seq increments a sequence number.
 #
  def do_inc_seq( self, value ):
    return value + 1  if value < 255 else 1

 #
 # Method do_syntax checks the syntax of an incoming command. It should be a
 # tuple containing the source, the payload and the time of arrival. The
 # function returns True iff the syntax is correct.
 #
  def do_syntax( self, extcmd ):
    assert type(extcmd) is tuple, 'Unexpected format of command'
    assert len(extcmd) == 3, 'Unexpected itemcount in tuple'
    (srcid,rawcmd,toa)= extcmd		# Unwrap tuple
  #
    if len(srcid)  == 0:  return False	# Illegal source identifier
    srcid= srcid.strip()		# Remove surrounding whitespace
    if len(srcid)  == 0:  return False	# Illegal source identifier
    if len(rawcmd) == 0:  return False	# Illegal command
    if len(rawcmd) > 127: return False	# Illegal command
  #
    return True

 #
 # Action do_cmd handles a command received via MQTT. The source identifier and
 # the sequence number are saved, a new sequence number is set, and the raw
 # command is forwarded to the next stage, that is thread Rfxcom.
 #
  def do_cmd( self, cmd ):
    if self.do_syntax( cmd ):
      (srcid,rawcmd,toa)= cmd		# Unwrap tuple
  #
      if type(rawcmd) is bytes:
        rawcmd= bytearray( rawcmd )	# Make mutable list of bytes
      self.cmdisn= rawcmd[2]		# Extract sequence number
      rawcmd[2]= 0			# Reset sequence number
      self.cmdraw= rawcmd
      self.cmdsrc= srcid
      self.cmdtoa= toa
      if srcid not in self.cmdsts:
        self.cmdsts[srcid]= { 'packets':0, 'octets':0, 'sumtime':0 }
      self.cmdsts[srcid]['packets']+= 1	# Update source statistics
      self.cmdsts[srcid]['octets'] += 1 + len(rawcmd)
  #
      self.cmdraw[2]= self.cmdesn	# Set egress sequence number
      self.rspisn   = self.cmdesn	# Expected response sequence number
      self.cmdesn   = self.do_inc_seq( self.cmdesn )
      self.cmdwdt.reset()		# Start watchdog timer
  #
      RfxQueCmd.put( self.cmdraw )	# Send cmd to RFXcom
    else:
      self.LogMessage( 'Illegal syntax: <{}>'.format(cmd) )
      self.errsts['cmd_errors']+= 1	# Update error statistics

 #
 # Action do_msg handles the receipt of an unsolicited response a.k.a. message.
 #
  def do_msg( self, msg ):
    if msg[0] < 0x03:
      self.LogMessage( 'Illegal unsolicited response: <{}>'.format(msg) )
      self.errsts['msg_errors']+= 1	# Update error statistics
      return
#   msg[2]= 0				 # Clear sequence number
    self.msgsts['packets']+= 1
    self.msgsts['octets' ]+= 1 + len(msg)
    MqtQueMsg.put( msg )
    return

 #
 # Action do_rsp handles a response, which might be unsolicited, from the
 # RFXcom transceiver.
 #
  def do_rsp( self, rsp ):
    if rsp[0] > 0x02:			# Check for an unsolicited message
      self.do_msg( rsp )
      self.dfsm.AugmentEvent( FsmStiMsg )
      return
  #
    if rsp[2] == self.rspisn:
      self.cmdwdt.stop()		# Stop the watchdog timer
      rsp[2]= self.cmdisn		# Restore original sequence number
      rsp= ( self.cmdsrc, rsp )
      MqtQueRsp.put( rsp )		# Move to thread Mqtt
      self.feednc.set()			# Allow for next command
      self.cmdsts[self.cmdsrc]['sumtime']+= time.time() - self.cmdtoa

  # The sequence number is not correct. The error is only reported. As the
  # watchdog timer is still running, it will cause a NACK to be sent if no
  # response with the correct sequence number comes in.
    else:
      self.LogMessage( 'Unexpected sequence number: <{}>'.format(rsp) )
      self.errsts['rsp_errors']+= 1	# Update statistics

 #
 # Action do_timeout sends a negative acknowledge response on the last command,
 # as no response has been received from the RFXcom transceiver (in time).
 #
  def do_timeout( self ):
    rsp= bytearray(RfxRspNegAck)	# Negative acknowledge response
    rsp[2]= self.cmdisn			# Set sequence number
    rsp= ( self.cmdsrc, rsp )
    MqtQueRsp.put( rsp )		# Move to thread Mqtt
    self.feednc.set()			# Allow for next command
    self.cmdsts[self.cmdsrc]['sumtim']+= time.time() - self.cmdtoa

  def __init__( self, *args, **kwargs ):
    super().__init__( *args, **kwargs )

    self.fsmtm= {			# Finite state machine transition matrix
      'Init' : {
        FsmStiCmd : ( 'Wait'  , self.do_cmd     ),
        FsmStiRsp : ( 'Init'  , self.do_msg     ),
        FsmStiMsg : ( 'Revert', self.do_nothing ),
        FsmStiTim : ( 'Init'  , self.do_nothing ),
      },
      'Wait' : {
        FsmStiCmd : ( 'Wait'  , self.do_error   ),
        FsmStiRsp : ( 'Init'  , self.do_rsp     ),
        FsmStiMsg : ( 'Revert', self.do_error   ),
        FsmStiTim : ( 'Init'  , self.do_timeout ),
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
    self.cmdtoa= None			# Current command: time of arrival
    self.cmdwdt= watchdog.WatchdogTimer( 3.5, self.on_timeout )
    self.rspisn= 1			# Next response: sequence number
    self.cmdsts= {}			# Statistics per source
    self.msgsts= { 'packets':0, 'octets':0 }	# Statistics of the unsolicited responses
    self.errsts= dict(
      fsm_errors= 0,			# FSM detected errors
      cmd_errors= 0,			# Command syntax error
      rsp_errors= 0,			# Response sequence number error
      msg_errors= 0,			# Message error
      time_outs = 0,			# Response missing / too late
    )

 #
 # Method cmd_feeder is started as a thread. If the state allows, that is if
 # event self.feednc is set, it gets the next command from queue MqtQueCmd and
 # passes it, with the appropriate stimulus, to the FSM. Upon receipt of the
 # associated response, event self.feednc will be set.
 #
  def cmd_feeder( self ):
    while not self.stopped():
      self.feednc.wait()		# Wait for clearance
      self.feednc.clear()		# Remove clearance
      if self.stopped():  break		# Exit if script needs to stop
      cmd= MqtQueCmd.get()		# Fetch next command
      if cmd is None:  break		# Exit if script needs to stop
      self.dfsm.HandleEvent( FsmStiCmd, cmd )

 #
 # Method rsp_feeder gets the next response from the RFXcom transceiver and
 # passes it, with the appropriate stimulus, to the FSM.
 #
  def rsp_feeder( self ):
    rsp= RfxQueRsp.get()		# Fetch next response
    if rsp is None:  return		# Exit is script needs to stop
    self.dfsm.HandleEvent( FsmStiRsp, rsp )

 #
 # Method loop is the 'main program' of this class / thread.
 #
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

 #
 # Method stop stops this thread. Dummy messages are put in queues to unblock
 # Queue.get method invocations.
 #
  def stop( self ):
    super().stop()
    self.feednc.set()			# Unblock threads of this thread
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
  '''Interface between dispatcher and the RFXcom transceiver.'''

  def __init__( self, *args, **kwargs ):
    super().__init__( *args, **kwargs )
    self.port  = None			# Serial port name
    self.serial= None			# Serial port object instance
    self.thread= None			# Thread reading from serial port
    self.state = 'init'			# Operational state of thread
    self.seqnbr= 0			# Packet sequence number
    self.fails = 0			# Count of successive failures
    self.device= dict(
      rfx_model = None,			# RFXcom device model name
      rfx_sn    = None,			# RFXcom device serial number
      rfx_hardwr= '',			# RFXcom hardware version number
      rfx_firmwr= '',			# RFXcom firmware version number
    )
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
    self.device['rfx_model']= None
    self.device['rfx_sn'   ]= None
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
          self.device['rfx_model']= mo.group(2)
          self.device['rfx_sn'   ]= mo.group(3)
          self.port= '/dev/' + mo.group(4)
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
    self.device['rfx_hardwr']= '{:d}.{:03d}'.format(bfr[10],bfr[11])  # msg7, msg8
    self.device['rfx_firmwr']= '{:02x}.{:03d}'.format(bfr[13],bfr[5]) # msg10, msg2
  # Set the set of enabled protocols to the default set.
    if bfr[6:10] != RfxMode:
      self.LogMessage( 'Activate default set of protocol decoders')
      bfr= bytearray(RfxCmdSetMode)	# Generic set-mode command
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
        RfxQueRsp.put( bfr )
        if _debug_mqtt_rfx_:
          self.LogMessage( 'rsp <{}>'.format(bfr) )
      if self.fails > RfxSerFT:
        RfxQueCmd.put( None )		# Inhibit blocking in method write
        self.state= 'restart'
        break

 #
 # Method write waits for a packet containing either a mode command or a device
 # command. The packet is written to the RFXcom transceiver. If writing fails a
 # NAK response is sent back to the source.
 #
  def write( self ):
    bfr= RfxQueCmd.get()
    if bfr is None:  return		# Wake up call, state change(d)
    if _debug_mqtt_rfx_:
      self.LogMessage( 'cmd <{}>'.format(bfr) )
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
          time.sleep( 10 )

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
    if self.serial is not None:
      self.serial.close()
    self.LogMessage( 'Stopping thread' )

 #
 # Method stop stops this thread. Note that method write needs to be unblocked,
 # but method read will unblock within 10 seconds.
 #
  def stop( self ):
    super().stop()
    RfxQueCmd.put( None )		# Unblock method write


#
# Class Controller.
# -----------------
#
# Class controller accepts some get-commands sent via MQTT, and replies with the
# requested information.
#
class Controller( ControlThread ):
  '''Return the status or the statistics of this bridge.'''

  def __init__( self, *args, **kwargs ):
    super().__init__( *args, **kwargs )
    self.srcid = None			# Source identifier

 #
 # Method loop waits for a topic containing a get command. The payload of this
 # topic is not looked at and is thus irrelevant. If the topic is recognised,
 # the requested information is sent to the requester.
 #
  def loop( self ):
    self.LogMessage( 'Starting thread' )
    while not self.stopped():
      rqst= CtlQueIng.get()
      if rqst is None:  continue	# Stop thread

      assert type(rqst) is tuple
      assert len(rqst) == 4
      (tpc,src,pld,now)= rqst
      if _include_push_  and  src == _push_srcid_:
        self.LogMessage( 'Received get command with wrong source identifier' )
        continue

      if   tpc == MqtTpcMuxGsu:
        self._send_status( src, now )
      elif tpc == MqtTpcMuxGsi:
        self._send_statistics( src, now )
      else:
        self.LogMessage( 'Unsupported topic: {}:<{}>'.format(src,tpc) )

    self.LogMessage( 'Stopping thread' )

  def stop( self ):
    super().stop()
    CtlQueIng.put( None )		# Unblock loop


#
# Class Reporter.
# ---------------
#
# Class Reporter reports periodically the state and the statistics to those who
# are subscribed to the topic. It uses thus a push mechanism. This class acts as
# if a (periodical) get request is received with source identifier _push_srcid_.
# Note that no class instantiation is created if installation constant
# _include_push_ has a False value.
#
class Reporter( ControlThread ):
  '''Report periodically the status and the statistics.'''

  def __init__( self, *args, **kwargs ):
    super().__init__( *args, **kwargs )
    self.srcid = _push_srcid_		# Destination identifier

  def loop( self ):
    self.LogMessage( 'Starting thread' )
    while not self.stopped():
      self._send_status    ( self.srcid, time.time() )
      self._send_statistics( self.srcid, time.time() )
      self.Wait( *_push_timing_ )	# Push every 5 minutes
    self.LogMessage( 'Stopping thread' )


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
    if _debug_mqtt_rfx_:
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
# or in an abnormal (exception) way. In either case, the flag to stop this
# script is set.
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
if _debug_mqtt_rfx_:
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
th3= Controller( OnTerminate, name='control'  ) ;  threads.append(th3) ;  th3.start()
if _include_push_:
  th4= Reporter( OnTerminate, name='report'   ) ;  threads.append(th4) ;  th4.start()

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

if _debug_mqtt_rfx_:
  dlfo.close()
