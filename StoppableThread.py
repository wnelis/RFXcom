#
# Class StoppableThread extends class threading.Thread with two options
# regarding stopping:
# A) Event _stop_event is added to the thread as well as methods to set and
#    check this event. The thread itself has to check regularly for the
#    stopped() condition.
# B) A call-back can be specified which is invoked at normal and abnormal
#    termination of the thread. This is incorporated to prevent the main thread
#    controller from polling the status of the thread(s) regularly.
#
# The main loop is normally put in method run(), but this method is already
# defined in this class and should not be redefined in a derived class. In stead
# a derived class should define a method named loop() containing the main loop.
#
# The call-back function is invoked with two parameters, the name of the thread
# and in case of an abnormal termination the description of the exception. In
# case of a normal termination, the second parameter is None.
#
# Moreover, method Wait (an extended version of time.sleep) is added, as it is
# needed by many subclasses. It is meant for cases in which the thread has to
# wait some fixed time but react fast when event _stop_event is set.
#
# This class is intended for use cases in which (A) the threads are to be active
# for extended periods of time, typically multiple days, and (B) the termination
# of one thread should cause all other threads, including the main thread, to
# stop too.
#
# NOTE: threading.excepthook can also handle the (uncaught) exceptions occurring
#       in Thread.run. However, it cannot handle the normal termination of a
#       thread.
#
import time
import threading

class StoppableThread( threading.Thread ):
  """Thread class with a stop() method and a call-back invoked at termination."""

  def __init__( self, callback=None, *args, **kwargs ):
    super().__init__( *args, **kwargs )
    self._stop_event  = threading.Event()
    self._on_terminate= callback

  def join( self, TimeOut=None ):
    self.stop()
    super().join( TimeOut )

 #
 # Abstract method loop is invoked by method StoppableThread.run. The
 # implementation below should be overridden in any derived class.
 #
  def loop( self ):
    raise NotImplementedError

 #
 # Method run is invoked by threading.Thread.start. This method invokes the
 # overridden method loop to do the actual work. Upon termination of method
 # loop, it will invoke the callback function.
 #
  def run( self ):
    try:
      self.loop()
    except Exception as e:
      if self._on_terminate is not None:
        self._on_terminate( self.name, str(e) )
    except:
      if self._on_terminate is not None:
        self._on_terminate( self.name, None )
    else:
      if self._on_terminate is not None:
        self._on_terminate( self.name, None )

  def stop( self ):
    self._stop_event.set()

  def stopped( self ):
    return self._stop_event.is_set()

  def wait( self, timeout ):
    return self._stop_event.wait( timeout )

 #
 # Method Wait waits until the (Unix) timestamp reaches the next integer
 # multiple of Period seconds and then another Delay seconds. However, if the
 # thread method stop is invoked before the time has expired, this method will
 # return at that time.
 # Parameters Period and Delay are integer numbers, with Period >= 2 and
 # 0 <= Delay < Period.
 #
  def Wait( self, Period, Delay ):
    assert Period >=    2 , 'Period is too small'
    assert Delay  >=    0 , 'Negative delays are not allowed'
    assert Delay  < Period, 'Delay too large'
    Now= time.time()
    ActTim= int( Now )
    ActTim= ( (ActTim+Period-1) // Period ) * Period
    SlpTim= ActTim - Now + Delay
    if SlpTim < 1.5:  SlpTim+= Period
    self.wait( SlpTim )

