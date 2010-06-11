# -*- Mode: Python; tab-width: 4 -*-

# Copyright (c) 1999, 2000 by eGroups, Inc.
# Copyright (c) 2005-2010 Slide, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#     * Neither the name of the author nor the names of other
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import pdb
import traceback
import bisect
import os
import select
import string
import signal
import sys
import time
import random as whrandom
import logging
import errno
import weakref

import _socket
import socket
#
# The creation of the ssl object passed into coroutine_ssl relies on
# a _ssl module which correctly handles non-blocking connect. (as of
# python 2.4.1 the shipping _ssl.so does not correctly support this
# so a patched version is in this source tree.)
#
try:
	import green_ssl as __real_ssl__
except:
	__real_ssl__ = None

try:
	import _epoll
except:
	_epoll = None

try:
    import itimer
except:
    itimer = None

# sentinel value used by wait_for_read() and wait_for_write()
USE_DEFAULT_TIMEOUT = -1
#
# poll masks.
DEFAULT_MASK = select.POLLIN|select.POLLOUT|select.POLLPRI
ERROR_MASK = select.POLLERR|select.POLLHUP|select.POLLNVAL

from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, ECONNRESET, ENOTCONN
import exceptions
#
# import underlying coroutine module: greenlet.
#
try:
	from greenlet import greenlet
except ImportError:
	from py.magic import greenlet
#
# save real socket incase we emulate.
#
__real_socket__ = _socket.socket

class CoroutineSocketError (exceptions.Exception):
	pass

class CoroutineCondError (exceptions.Exception):
	pass

class CoroutineThreadError (exceptions.Exception):
	pass

class TimeoutError (exceptions.Exception):
	pass

class CoroutineSocketWake (exceptions.Exception):
	pass

class CoroutineCondWake (exceptions.Exception):
	pass

class ConnectionError(Exception):
	pass

class LockTimeout(exceptions.Exception):
	pass

# ===========================================================================
#                          Coroutine Socket
# ===========================================================================

class coroutine_socket(object):
	'''coroutine_socket

	socket that automatically suspends/resumes instead of blocking.
	'''
	def __init__ (self, *args, **kwargs):
		"""Timeout semantics for __init__():
		   If you do not pass in a keyword arg for timeout (or
		   pass in a value of None), then the socket is blocking.
		"""

		self.socket = kwargs.get('sock', args and args[0] or None)
		if self.socket:
			self.socket.setblocking(0)
			self.set_fileno()

		self.family = kwargs.get('family', socket.AF_UNSPEC)

		me = current_thread()
		if me is not None:
			default = me.get_socket_timeout()
		else:
			default = None

		if default is None:
			default = getdefaulttimeout()

		self._timeout         = kwargs.get('timeout', default)
		self._connect_timeout = kwargs.get('connect_timeout', default)

		self._wake   = []
		self._closed = 0
		#
		# coro poll threads waiting for sig.
		self._waits  = {None: 0}

	def set_fileno (self):
		self._fileno = self.socket.fileno()

	def fileno (self):
		return self._fileno

	def create_socket (self, family, type, proto = 0):
		if self.family != family:
			self.family = family
		self.socket = __real_socket__(family, type, proto)
		self.socket.setblocking(0)
		self.set_fileno()

	def _wait_add(self, mask):
		self._waits[current_thread()] = mask
		return reduce(lambda x, y: x|y, self._waits.values())

	def _wait_del(self):
		del(self._waits[current_thread()])
		return reduce(lambda x, y: x|y, self._waits.values())

	def _waiting(self):
		return filter(lambda y: y[0] is not None, self._waits.items())

	def wake(self, mask = 0xffff):
		for thrd, mask in filter(lambda y: y[1] & mask, self._waiting()):
			if thrd is current_thread():
				raise CoroutineThreadError, 'cannot wakeup current thread'

			schedule(thrd)

	def _wait_for_event(self, eventmask, timeout):
		"""Timeout semantics:
		   No timeout keyword arg given means that the default
		   given to __init__() is used. A timeout of None means
		   that we will act as a blocking socket."""

		if timeout == USE_DEFAULT_TIMEOUT:
			timeout = self._timeout

		me = current_thread()
		if me is None:
			raise CoroutineSocketError, "coroutine sockets in 'main'"

		the_event_poll.register(self, eventmask)
		result = me.Yield(timeout, 0)
		the_event_poll.unregister(self)

		if result is None:
			raise CoroutineSocketWake, 'socket has been awakened'

		if result == 0:
			raise TimeoutError, "request timed out in recv (%s secs)" % timeout

		if 0 < (result & eventmask):
			return None

		if 0 < (result & ERROR_MASK):
			raise socket.error(socket.EBADF, 'Bad file descriptor')
		#
		# all cases should have been handled by this point
		return None

	def wait_for_read (self, timeout = USE_DEFAULT_TIMEOUT):
		return self._wait_for_event(select.POLLIN, timeout)

	def wait_for_write (self, timeout = USE_DEFAULT_TIMEOUT):
		return self._wait_for_event(select.POLLOUT, timeout)

	def connect (self, address):
		try:
			if socket.AF_INET == self.family:
				host, port = address
				#
				# Perform DNS resolution here so it will use our,
				# coroified resolver instead of the DNS code
				# baked into Python, which uses native sockets.
				#
				host = socket.gethostbyname(host)
				address = (host, port)

			return self.socket.connect(address)
		except socket.error, why:
			if why[0] in (errno.EINPROGRESS, errno.EWOULDBLOCK):
				pass
			elif why[0] == errno.EALREADY:
				return
			else:
				raise socket.error, why
		#
		# When connect gets a EINPROGRESS/EWOULDBLOCK exception, we wait
		# until the socket has completed the connection (ready for write)
		# Coroutine yields are done outside of the exception handler, to
		# avoid tracebacks leaking into other coroutines.
		#
		self.wait_for_write(timeout = self._connect_timeout)
		ret = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
		if ret != 0:
			raise socket.error, (ret, os.strerror(ret))

	def recv (self, buffer_size):
		self.wait_for_read()
		return self.socket.recv(buffer_size)

	def recvfrom (self, buffer_size):
		self.wait_for_read()
		return self.socket.recvfrom (buffer_size)

	def recv_into (self, buffer, buffer_size):
		self.wait_for_read()
		return self.socket.recv_into (buffer, buffer_size)

	def recvfrom_into (self, buffer, buffer_size):
		self.wait_for_read()
		return self.socket.recvfrom_into (buffer, buffer_size)

	# Things we try to avoid:
	#   1) continually slicing a huge string into slightly smaller pieces
	#     [e.g., 1MB, 1MB-8KB, 1MB-16KB, ...]
	#   2) forcing the kernel to copy huge strings for the syscall
	#
	# So, we try to send reasonably-sized slices of a large string.
	# If we were really smart we might try to adapt the amount we try to send
	# based on how much got through the last time.

	_max_send_block = 64 * 1024

	def send (self, data):
		self.wait_for_write()
		return self.socket.send(data)

	def sendall(self, data):
		t = 0
		while data:
			self.wait_for_write()
			n = self.socket.send(data[:min(len(data), self._max_send_block)])
			data = data[n:]
			t = t + n
		return t

	def sendto (self, data, where):
		self.wait_for_write()
		return self.socket.sendto (data, where)

	def bind (self, address):
		return self.socket.bind (address)

	def listen (self, queue_length):
		return self.socket.listen (queue_length)

	def accept (self):

		try:
			self.wait_for_read()
		except TimeoutError, e:
			raise socket.timeout, 'timed out'
		try:
			conn, addr = self.socket.accept()
		except socket.error, e:
			conn, addr = self.socket.accept()
		return self.__class__ (conn), addr

	def close (self):
		if not self._closed:
			self._closed = 1
			if self.socket:
				return self.socket.close()
			else:
				return None

	def shutdown(self, *args):
		return self.socket.shutdown(*args)

	def __del__ (self):
		self.close()

	def set_reuse_addr (self):
		# try to re-use a server port if possible
		try:
			self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		except:
			pass

	def settimeout(self, o):
		self._connect_timeout = o
		self._timeout         = o

	def gettimeout(self):
		return self._timeout

	def makefile(self, mode='r', bufsize=-1):
		return socket._fileobject(self, mode, bufsize)

	def setsockopt(self, level, option, value):
		return self.socket.setsockopt(level, option, value)

	def getsockopt(self, level, option):
		return self.socket.getsockopt(level, option)

	def getsockname(self):
		return self.socket.getsockname()


SSL_WAIT_ERRORS = set([
	socket._ssl.SSL_ERROR_WANT_READ,
	socket._ssl.SSL_ERROR_WANT_WRITE,
	socket._ssl.SSL_ERROR_WANT_CONNECT,
])
	
class coroutine_ssl(object):
	def __init__(self, socket, ssl):
		self._socket = socket
		self._ssl = ssl
		
	def read(self, size):
		data = ''
		error = 0
		while size:
			#
			# Yield on initial entry (no error set) to prevent non-yielding
			# read loops using small increments to fetch large amounts of
			# data. Error set to want write or connect is a wait for file
			# descriptor write ready.
			#
			if not error:
				current_thread().Yield(0.0)
			elif error == socket._ssl.SSL_ERROR_WANT_READ:
				self._socket._sock.wait_for_read()
			else:
				self._socket._sock.wait_for_write()

			try:
				partial = self._ssl.read(size)
			except (socket.error, socket.sslerror), exception:
				error = exception[0]

				if error in SSL_WAIT_ERRORS:
					continue
				elif data:
					break
				else:
					raise

			if not partial:
				break

			error = 0
			size -= len(partial)
			data += partial

		return data

	def write(self, data):
		error = 0
		sent = 0
		while data:
			#
			# see read
			#
			if not error:
				current_thread().Yield(0.0)
			elif error == socket._ssl.SSL_ERROR_WANT_READ:
				self._socket._sock.wait_for_read()
			else:
				self._socket._sock.wait_for_write()

			try:
				size = self._ssl.write(data)
				sent += size
			except (socket.error, socket.sslerror), exception:
				error = exception[0]

				if error in SSL_WAIT_ERRORS:
					continue
				elif sent:
					break
				else:
					raise

			error = 0
			data  = data[size:]

		return sent

	def issuer(self):
		return self._ssl.issuer()

	def server(self):
		return self._ssl.server()

# ===========================================================================
#                         Condition Variable
# ===========================================================================

class coroutine_cond(object):
	__slots__ = ['_waiting', '_ordered', '_counter']

	def __init__ (self):
		self._waiting = {}
		self._ordered = []
		self._counter = 0

	def __len__(self):
		return self._counter

	def wait (self, timeout = None):
		thrd = current_thread()
		tid  = thrd.thread_id()

		self._counter += 1
		self._waiting[tid] = thrd
		self._ordered.append(tid)

		result = thrd.Yield(timeout)
		#
		# If we have passed to here, we are not waiting any more.
		# depending how we were awoken, we may need to remove the
		# thread reference
		#
		if self._waiting.has_key(tid):
			del self._waiting[tid]
			self._ordered.remove(tid)

		self._counter -= 1
		return result

	def wake (self, id, *args):
		thrd = self._waiting.pop(id, None)
		if thrd is None:
			raise CoroutineCondError, 'unknown thread <%d>' % (id)

		self._ordered.remove(id)
		schedule (thrd, args)

	def wake_one (self, *args):
		if not len(self._waiting):
			return None

		thid = self._ordered.pop(0)
		thrd = self._waiting.pop(thid)

		schedule(thrd, args)

	def wake_all (self, *args):
		for thrd in self._waiting.values():
			schedule (thrd, args)

		self._waiting = {}
		self._ordered = []

class stats_cond(coroutine_cond):
	def __init__ (self, timeout = None):
		super(self.__class__, self).__init__()
		self.waiting     = {}
		self.total_waits = 0
		self.total_time  = 0.0
		self.timeouts    = 0
		self.timeout     = timeout

	def wait (self, timeout = None):
		tid = current_thread().thread_id()
		self.waiting[tid] = time.time()

		if timeout is None:
				timeout = self.timeout

		result = super(self.__class__, self).wait(timeout)
		if result is None:
			self.timeouts += 1

		self.total_waits += 1
		self.total_time  += time.time() - self.waiting[tid]

		del self.waiting[tid]
		return result

	def stats(self):
		result = {
			'waiting': self.waiting.values(),
			'timeouts': self.timeouts,
			'total': self.total_time,
			'waits': self.total_waits }

		result['waiting'].sort()

		if self.total_time:
			result.update({'average': self.total_time/self.total_waits })
		else:
			result.update({'average': 0})

		return result;

class coroutine_lock(object):
	__slots__ = ['_cond', '_lock']

	def __init__(self):
		self._cond = coroutine_cond()
		self._lock = 0

	def __len__(self):
		return len(self._cond)

	def lock(self, timeout = None):
		while self._lock:
			result = self._cond.wait(timeout)
			if result is None:
				raise LockTimeout('lock timeout', timeout)

		self._lock = 1

	def tset(self):
		locked, self._lock = bool(self._lock), 1
		return not locked

	def unlock(self):
		self._lock = 0
		self._cond.wake_one()

	def locked(self):
		return bool(self._lock)

	def mucked(self):
		return bool(self._lock or len(self._cond))


class coroutine_fifo(object):
	def __init__(self, timeout = None):
		self._cond = coroutine_cond()
		self._timeout = timeout
		self._fifo = []

	def __len__(self):
		return len(self._fifo)

	def waiters(self):
		return len(self._cond)

	def push(self, o):
		self._fifo.append(o)
		self._cond.wake_one(None)

	def pop(self, **kwargs):
		timeout = kwargs.get('timeout', self._timeout)

		while not len(self._fifo):
			result = self._cond.wait(timeout = timeout)
			if result is None:
				raise TimeoutError
			elif not result:
				raise CoroutineCondWake

		return self._fifo.pop(0)

	def wake(self):
		self._cond.wake_all()

# ===========================================================================
#                Coro-aware Logging and Standard IO replacements
# ===========================================================================
class coroutine_logger(logging.Logger):
	def makeRecord(self, *args):
		args = args[:7]
		rv = logging.LogRecord(*args)
		rv.__dict__.update({'coro':current_id()})
		return rv

class coroutine_stdio(object):
	def __init__(self):
		self._partial = {}

	def write(self, str):
		value = string.split(str, '\n')
		id    = current_id()

		self._partial[id] = self._partial.get(id, '') + value[0]
		value = value[1:]

		if not value:
			return

		self._output(self._prefix + self._partial[id])
		for line in value[:-1]:
			self._output(self._prefix + line)

		self._partial[id] = value[-1]
		return

	def flush(self):
		pass

class coroutine_stdout(coroutine_stdio):
	def __init__(self, log):
		super(self.__class__,self).__init__()
		self._output = log.info
		self._prefix = '>> '

class coroutine_stderr(coroutine_stdio):
	def __init__(self, log):
		super(self.__class__,self).__init__()
		self._output = log.error
		self._prefix = '!> '


# ===========================================================================
#                     Thread Abstraction
# ===========================================================================

def get_callstack(depth = 2):
	data  = []

	while depth is not None:
		try:
			f = sys._getframe(depth)
		except:
			depth = None
		else:
			data.append((
				'/'.join(f.f_code.co_filename.split('/')[-2:]),
				f.f_code.co_name,
				f.f_lineno))
			depth += 1

	return data

def preemptive_locked():
	'''preemptive_locked

	Decorator for functions which need to execute w/ preemption disabled.
	'''
	def function(method):
		def disabled(obj, *args, **kwargs):
			obj.preemptable_disable()
			try:
				return method(obj, *args, **kwargs)
			finally:
				obj.preemptable_enable()

		return disabled
	return function

_current_threads = {}

class Thread(object):
	_thread_count = 0

	def __init__(self, *args, **kwargs):
		self._thread_id = Thread._thread_count = Thread._thread_count + 1

		if not kwargs.has_key('name'):
			self._name = 'thread_%d' % self._thread_id
		else:
			self._name = kwargs['name']

		self._target = kwargs.get('target', None)
		self._args   = kwargs.get('args', ())
		self._kwargs = kwargs.get('kwargs', {})
		#
		# statistics when profiling
		#
		self._resume_count = 0 
		self._total_time   = 0
		self._long_time    = 0

		self._alive = 0
		self._started = 0
		self._profile = 0
		self._daemonic = 0
		self._deadman = None
		self._joinees = None
		self._locald  = {}
		#
		# preemptive scheduler info
		#
		self._preempted   = 0
		self._preemptable = [1, 0]
		#
		# debug info
		#
		self._trace = 0
		self._where = None
		#
		# child management
		#
		self._children = {}
		self._cwaiter  = coroutine_cond()
		## dp 8/16/05
		## self._co = coroutine.new (self._run, 65536)
		self._co = greenlet(self._run)

		self._status = 'initialized'
		#
		# inherit some properties from a parent thread
		#
		parent = current_thread()
		if parent:
			self._log      = kwargs.get('log', parent._log)
			self._loglevel = parent._loglevel
			self._profile  = parent._profile

			def dropped(ref):
				self._parent = None

			self._parent = weakref.ref(parent)
		else:
			self._log      = kwargs.get('log', None)
			self._loglevel = logging.DEBUG
			self._parent   = None
		#
		# call user specified initialization function
		#
		self.init()

	def __del__ (self):
		if self._alive:
			self.kill()

	def __repr__ (self):
		if self._profile:
			p = ' resume_count: %d execute_time: %s' % (
				self._resume_count,
				self._total_time)
		else:
			p = ''
		if self._alive:
			a = 'running'
		else:
			a = 'suspended'

		return '<%s.%s id:%d %s %s%s at %x>' % (
			__name__,
			self.__class__.__name__,
			self._thread_id,
			self._status,
			a,
			p,
			id(self))
	#
	# subclass overrides
	#
	def init(self):
		'user defined initialization call'
		pass

	def complete(self):
		'user defined cleanup call on completion'
		pass

	def run (self):
		sys.stderr.write('Unregistered run method!\n')
	#
	# intra-thread management
	#
	def child_join(self, child):
		'''child_join

		children register with their parent at initialization time
		'''
		tid = child.thread_id()
		self._children[tid] = tid

	def child_drop(self, child):
		'''child_drop

		children unregister from their parent when they are destroyed
		'''
		o = self._children.pop(child.thread_id(), None)
		if o is not None:
			self._cwaiter.wake_all()

	def child_list(self):
		return map(lambda t: _get_thread(t), self._children.keys())

	def child_wait(self, timeout = None, exclude = set()):
		while set(self._children.keys()) - exclude:
			start  = time.time()
			result = self._cwaiter.wait(timeout)
			if result is None:
				break

			if timeout is not None:
				timeout -= time.time() - start

		return self.child_count() - len(exclude)

	def child_count(self):
		return len(self._children)

	def parent(self):
		if self._parent is not None:
			return self._parent()
		else:
			return None

	def abandon(self):
		for tid in self._children.values():
			child = _get_thread(tid)
			if child is not None:
				child._parent = None

		self._children = {}
		self._cwaiter.wake_all()

	def start (self):
		if not self._started:
			self._started = 1
			schedule(self)

	def resume (self, args):
		if not hasattr(self, '_co'):
			return

		self._co.parent = MAIN_EVENT_LOOP
		#
		# clear preemption data on resume not on completion.
		#
		self._preempted = 0

		if self._profile:
			self._resume_count = self._resume_count + 1
			start_time = time.time()
		else:
			start_time = 0

		try:
			if self._alive:
				# This first one will create a self-referenced tuple,
				# which causes a core dump. The second does not.
				# Extremely weird.
				# result = coroutine.resume (self._co, (args,))
				# dp 8/16/05
				# newargs = (args,)
				# coroutine.resume (self._co, newargs)
				result = self._co.switch(args)
			else:
				result = self._co.switch()
		except greenlet.GreenletExit:
			self.kill()

		if self._profile and start_time:
			exec_time = time.time() - start_time

			self._total_time += exec_time
			self._long_time   = max(self._long_time, exec_time)

		return result

	def _run (self):
		global _current_threads
		try:
			self._alive   = 1
			self._status  = 'alive'
			self._joinees = []
			_current_threads[self._co] = self

			parent = self.parent()
			if parent is not None:
				parent.child_join(self)

			if self._target is None:
				result = apply (self.run, self._args, self._kwargs)
			else:
				result = apply (self._target, self._args, self._kwargs)
		except greenlet.GreenletExit:
			pass
		except:
			self.traceback()

		self.kill()

	def kill (self):
		if not self._alive:
			return

		self.complete()
		self.abandon()

		parent = self.parent()
		if parent is not None:
			parent.child_drop(self)

		if self._joinees is not None:
			for waiter in self._joinees:
				waiter.wake_all()

			self._joinees = None

		if _current_threads.has_key(self._co):
			del(_current_threads[self._co])

		self._alive  = 0
		self._status = 'dead'
		del(self._co)
	#
	# logging.
	#
	def set_log_level(self, level):
		self._loglevel = level
	def get_log_level(self):
		return self._loglevel
	def log(self, level, msg, *args):
		if not level < self._loglevel:
			if self._log:
				self._log.log(level, args and msg % args or msg)
			else:
				sys.stderr.write(
					'LOGLEVEL %02d: %s\n' % (level, args and msg % args or msg))
	def debug(self, msg, *args):
		self.log(logging.DEBUG, msg, *args)
	def info(self, msg, *args):
		self.log(logging.INFO, msg, *args)
	def warn(self, msg, *args):
		self.log(logging.WARN, msg, *args)
	def error(self, msg, *args):
		self.log(logging.ERROR, msg, *args)
	def critical(self, msg, *args):
		self.log(logging.CRITICAL, msg, *args)
	def traceback(self, level = logging.ERROR):
		t,v,tb = sys.exc_info()
		self.log(level, 'Traceback: [%s|%s]' % (str(t),str(v)))
		while tb:
			self.log(level, '  %s (%s:%s)' % (
				tb.tb_frame.f_code.co_filename,
				tb.tb_frame.f_code.co_name,
				str(tb.tb_lineno)))
			tb = tb.tb_next
		del(tb)
	#
	# thread local storage
	#
	def get_local(self, key, default = None):
		return self._locald.get(key, default)

	def pop_local(self, key, default = None):
		return self._locald.pop(key, default)
			
	def set_local(self, key, value):
		self._locald[key] = value

	def has_local(self, key):
		return self._locald.has_key(key)

	def set_locals(self, **kwargs):
		self._locald.update(kwargs)
		
	#
	#
	#
	@preemptive_locked()
	def Yield(self, timeout = None, arg = None):
		#
		# add self to timeout event list
		#
		if timeout is not None:
			if type(timeout) == type(0.0):
				now = time.time()
			else:
				now = int(time.time())

			triple = the_event_list.insert_event(self, now + timeout, arg)
		#
		# in debug mode record stack
		#
		if self._trace:
			self._where = get_callstack()
		#
		# release control
		#
		result = MAIN_EVENT_LOOP.switch()  ##coroutine.main(())
		#
		# remove self from timeout event list.
		#
		if timeout is not None:
			the_event_list.remove_event(triple)

		return result
	
	@preemptive_locked()
	def preempt(self, limit = 0, level = 1):
		if not self._preempted > max(limit, 1):
			return None

		self.Yield(0.0)

		if not level:
			return None
		#
		# debug output.
		#
		frame = sys._getframe(3)

		level -= 1
		if not level:
			self.info('Preempted:   %s  (%s:%d)' % (
				'/'.join(frame.f_code.co_filename.split('/')[-2:]),
				frame.f_code.co_name,
				frame.f_lineno))
			return None

		level -= 1
		if not level:
			self.info('Preempted:   %s' % (
				'|'.join(map(
					lambda i: '%s:%s()' % (i[0],i[1]),
					get_callstack(3)[:-2]))))
			return None

		self.info('Preempted:', pre)
		for data in get_callstack(3):
			thrd.info('    %s  (%s:%d)' % data)

		return None

	def join(self, timeout = None):
		'''join

		Wait, no longer then timeout, until this thread exits.

		results:
		  True  - successfully joined
		  False - join failed due to timeout
		  None  - join failed because coroutine is not joinable.
		'''
		waiter = coroutine_cond()
		if self._joinees is not None:
			self._joinees.append(waiter)
			result = waiter.wait(timeout)
			result = ((result is None and [False]) or [True])[0]
		else:
			result = None

		return result
	#
	# basic thread info
	#
	def profile(self, status, children = False):
		'''profile

		Enable/Disable the collection of the context switch counter and
		the execution time counter. Optionally cascade to child threads
		'''
		self._profile = status

		if not children:
			return None

		for child in self.child_list():
			child.profile(status, children = children)

	def profile_clear(self, children = False):
		'''profile_clear

		Reset the threads context switch counter and the execution time
		counters.
		'''
		self._resume_count = 0
		self._total_time   = 0
		self._long_time    = 0

		if not children:
			return None

		for child in self.child_list():
			child.clear(children = children)

	def clear(self):
		return self.profile_clear() # backwards code compat.

	def resume_count(self):
		'''resume_count

		Return the value of the context switch counter
		'''
		return self._profile and self._resume_count or None

	def total_time(self):
		'''total_time

		Return the value of the execution time counter values.
		'''
		return self._profile and self._total_time or None

	def long_time(self):
		'''long_time

		Return the time value of the longest executing section.
		'''
		return self._profile and self._long_time or None

	def trace(self, status):
		'''trace

		Enable/Disable the recording of the threads execution stack during
		the	most recent context switch.
		'''
		#
		# to allow orthogonal components to enable/disable trace, the
		# variable is implemented as a counter.
		#
		self._trace += (-1,1)[int(bool(status))]
		self._trace  = max(self._trace, 0)

	def where(self):
		'''where

		Return the execution stack of the thread at the time of the thread's
		last context switch.
		'''
		return self._where

	def thread_id (self):
		return self._thread_id

	def getName (self):
		return self._name

	def setName (self, name):
		self._name = name

	def isAlive (self):
		return self._alive

	def isDaemon (self):
		return self._daemonic

	def setDaemon (self, daemonic):
		self._daemonic = daemonic

	def status (self):
		print 'Thread status:'
		print '	id:          ', self._thread_id
		print '	alive:       ', self._alive
		if self._profile:
			print '	resume count:', self._resume_count
			print '	execute time:', self._total_time

	def set_socket_timeout(self, value):
		'''set_socket_timeout

		set a timer that no socket wait will exceed. This ensures that
		emulated sockets which use no timeout will not sleep indefinetly.
		'''
		self._deadman = value

	def get_socket_timeout(self):
		return self._deadman
	#
	# preemption support
	#
	def preemptable_disable(self):
		'''preemptable_disable

		Disable any preemption of this thread, regardless of other
		preemption settings. Primary usage is critical sections of
		lock code where atomicity needs to be guaranteed.

		See Also: preemptable_enable(), preemptable()
		'''
		self._preemptable[0] -= 1

	def preemptable_enable(self):
		'''preemptable_enable

		Disable any preemption of this thread, regardless of other
		preemption settings. Primary usage is critical sections of
		lock code where atomicity needs to be guaranteed.

		See Also: preemptable_disable(), preemptable()
		'''
		self._preemptable[0] += 1

	def preemptable_set(self, status):
		'''preemptable_set

		Set the thread level preemption enable/disable advisory. Multiple
		calls to enable (or disable) are cumulative. (i.e. the same
		number of disable (or enable) calls need to be made to reverse
		the action.)

		For example; Two enable calls followed by a single disable call
		will leave the thread in a state where the system is advised
		that preemption is allowed.

		See Also: preemptable_get(), preemptable()
		'''
		self._preemptable[1] += [-1, 1][int(bool(status))]

	def preemptable_get(self):
		'''preemptable_get

		Get the thread level  preemption enable/disable advisory.

		See Also: preemptable_set(), preemptable()
		'''
		return self._preemptable[1] > 0

	def preemptable(self):
		'''preemptable

		Return the preemptability state of the thread. Takes into account
		the advisory as well as the hard setting.

		NOTE: System level preemption needs to be enabled for this to
		      have any meaningful effect.

		See Also: coro.preemptive_enable(), coro.preemptive_disable()
		          preemptable_enable(), preemptable_disable(),
				  preemptable_set(), preemptable_get()
		'''
		return bool(self._preemptable[1] > 0 and self._preemptable[0])
#
# end class Thread
#
def traceback_info():
	t,v,tb = sys.exc_info()
	tbinfo = []

	while tb:
		tbinfo.append((
			tb.tb_frame.f_code.co_filename,
			tb.tb_frame.f_code.co_name,
			str(tb.tb_lineno)
			))
		tb = tb.tb_next
	del tb
	return (str(t), str(v), tbinfo)

def compact_traceback ():
	t,v,tb = traceback_info()

	if tb:
		file, function, line = tb[-1]
		info  = '['
		info += string.join(map(lambda x: string.join (x, '|'), tb), '] [')
		info += ']'
	else:
		file, function, line = ('','','')
		info = 'no traceback!'

	return (file, function, line), t, v, info
#
# ===========================================================================
#                   global state and threadish API
# ===========================================================================

default_timeout = None
# coroutines that are ready to run
pending = {}

def setdefaulttimeout(timeout):
	global default_timeout
	default_timeout = timeout

def getdefaulttimeout():
	global default_timeout
	return default_timeout

def make_socket (family, type, **kwargs):
	s = apply(coroutine_socket, (), kwargs)
	s.create_socket(family, type)
	return s

def new (function, *args, **kwargs):
	return Thread (target=function, args=args, kwargs=kwargs)

def new_socket(family, type, proto = 0):
	return coroutine_socket(sock = __real_socket__(family, type, proto))

def new_ssl(sock, keyfile=None, certfile=None):
    if not __real_ssl__:
        raise RuntimeError("the green_ssl extension is required")
	return coroutine_ssl(
		sock, __real_ssl__.ssl(sock._sock.socket, keyfile, certfile))

_emulate_list = [
	('socket', '_realsocket'),
	('socket', 'setdefaulttimeout'),
	('socket', 'getdefaulttimeout'),
	('socket', 'ssl'),
	('socket', 'sslerror'),
]
_original_emulate = {}
def socket_emulate():

	# save _emulate_list
	for module, attr in _emulate_list:
		_original_emulate.setdefault(
			(module, attr),
			getattr(sys.modules[module], attr, None))

	socket._realsocket = new_socket
	socket.setdefaulttimeout = setdefaulttimeout
	socket.getdefaulttimeout = getdefaulttimeout
	socket.ssl = new_ssl
    if __real_ssl__:
        socket.sslerror = __real_ssl__.sslerror

def socket_reverse():
	for module, attr in _emulate_list:
		if _original_emulate.get((module, attr)):
			setattr(sys.modules[module], attr, _original_emulate[(module, attr)])

def real_socket(family, type, proto = 0):
	# return the real python socket, can be useful when in emulation mode.
	return __real_socket__(family, type, proto)

def spawn (function, *args, **kwargs):
	t = Thread (target=function, args=args, kwargs=kwargs)
	t.start()
	return t

def schedule (coroutine, args=None):
	"schedule a coroutine to run"
	pending[coroutine] = args

def Yield(timeout = None):
	return current_thread().Yield(timeout)

def sleep(timeout):
	return current_thread().Yield(timeout)

def thread_list():
	return _current_threads.values()

def _get_thread(id):
	return (filter(lambda x: x._thread_id == id, thread_list()) + [None])[0]

def gt(tid = 1):
	return _get_thread(tid)

def current_thread(default = None):
	co = greenlet.getcurrent()
	return _current_threads.get (co, default)

def current_id():
	co = greenlet.getcurrent()
	thrd = _current_threads.get (co, None)
	if thrd is None:
			return -1
	else:
			return thrd.thread_id()

current = current_thread

def insert_thread(thrd):
	thrd.start()

def profile_start():
	return len([thrd.profile(True) for thrd in thread_list()])

def profile_stop():
	return len([thrd.profile(False) for thrd in thread_list()])

def profile_clear():
	return len([thrd.clear() for thrd in thread_list()])

def trace_start():
	return len([thrd.trace(True) for thrd in thread_list()])

def trace_stop():
	return len([thrd.trace(False) for thrd in thread_list()])

def trace_dump():
	return [(thrd.thread_id(), thrd._where) for thrd in thread_list()]
#
# thread local storage
#
class DefaultLocalStorage(object):
	def __init__(self):
		self._locald = {}

	def get_local(self, key, default = None):
		return self._locald.get(key, default)

	def pop_local(self, key, default = None):
		return self._locald.pop(key, default)
			
	def set_local(self, key, value):
		self._locald[key] = value

	def set_locals(self, **kwargs):
		self._locald.update(kwargs)

	def has_local(self, key):
		return self._locald.has_key(key)

default_local_storage = DefaultLocalStorage()

def get_local(key, default = None):
	return current_thread(default_local_storage).get_local(key, default)

def pop_local(key, default = None):
	return current_thread(default_local_storage).pop_local(key, default)
			
def set_local(key, value):
	return current_thread(default_local_storage).set_local(key, value)

def has_local(key):
	return current_thread(default_local_storage).has_local(key)

def set_locals(**kwargs):
	return current_thread(default_local_storage).set_locals(**kwargs)

def preemptable_disable():
	if not current_thread():
		return
	return current_thread().preemptable_disable()

def preemptable_enable():
	if not current_thread():
		return
	return current_thread().preemptable_enable()
#
# execute current pending coroutines. (run loop)
#
def run_pending():
	"run all pending coroutines"
	while len(pending):
		try:
			# some of these will kick off others, thus the loop
			c,v = pending.popitem()
			c.resume(v)
		except:
			# XXX can we throw the exception to the coroutine?
			traceback.print_exc()

class LoggingProxy(object):
	def log(self, level, msg, *args):
		if not current_thread():
			return sys.stderr.write(
				'LOGLEVEL %02d: %s\n' % (level, args and msg % args or msg))
		return current_thread().log(level, msg, *args)
	def debug(self, msg, *args):
		return self.log(logging.DEBUG, msg, *args)
	def info(self, msg, *args):
		return self.log(logging.INFO, msg, *args)
	def warn(self, msg, *args):
		return self.log(logging.WARN, msg, *args)
	def error(self, msg, *args):
		return self.log(logging.ERROR, msg, *args)
	def critical(self, msg, *args):
		return self.log(logging.CRITICAL, msg, *args)
	def traceback(self, level = logging.ERROR):
		if not current_thread():
			t,v,tb = sys.exc_info()
			self.log(level, 'Traceback: [%s|%s]' % (str(t),str(v)))
			while tb:
				self.log(level, '  %s (%s:%s)' % (
					tb.tb_frame.f_code.co_filename,
					tb.tb_frame.f_code.co_name,
					str(tb.tb_lineno)))
				tb = tb.tb_next
			del(tb)
			return
		return current_thread().traceback()

log = LoggingProxy()
#
#
# optional preemptive handler
#
the_preemptive_rate    = None
the_preemptive_count   = 2
the_preemptive_info    = 1

def preemptive_signal_handler(signum, frame):
	try:
		global the_preemptive_rate
		if the_preemptive_rate is None:
			return None

		thrd = current_thread()
		if thrd is None:
			return None

		thrd._preempted += 1

		if not thrd.preemptable():
			return None

		global the_preemptive_info
		global the_preemptive_count

		thrd.preempt(limit = the_preemptive_count, level = the_preemptive_info)
	except Exception, e:
		log.traceback()

def preemptive_enable(frequency = 50):
	global the_preemptive_rate

    if not itimer:
        raise RuntimeError("the 'itimer' extension is required for preempting")

	if the_preemptive_rate is None:
		signal.signal(signal.SIGPROF, preemptive_signal_handler)

	the_preemptive_rate = 1.0/frequency

	itimer.setitimer(
		itimer.ITIMER_PROF,
		the_preemptive_rate,
		the_preemptive_rate)

def preemptive_disable():
	global the_preemptive_rate

    if not itimer:
        return

	the_preemptive_rate = None

	itimer.setitimer(itimer.ITIMER_PROF, 0, 0)
	signal.signal(signal.SIGPROF, signal.SIG_IGN)


class event_list(object):

	def __init__ (self):
		self.events = []

	def __nonzero__ (self):
		return len(self.events)

	def __len__ (self):
		return len(self.events)

	def insert_event (self, co, when, args):
		triple = (when, co, args)
		bisect.insort (self.events, triple)

		return triple

	def remove_event (self, triple):
		offset = bisect.bisect_left(self.events, triple)
		#
		# If the triple exists in the event list remove it. The consumer
		# will attempt to remove an event that was removed by run_scheduled
		# during scheduling.
		#
		try:
			if self.events[offset] == triple:
				del(self.events[offset])
		except (ValueError, IndexError):
			pass

	def run_scheduled (self):
		now = time.time()
		i = j = 0
		while i < len(self.events):
			when, thread, args = self.events[i]
			if now >= when:
				schedule (thread, args)
				j = i + 1
			else:
				break
			i = i + 1

		del(self.events[:j])
		return None

	def next_event (self, max_timeout=30.0):
		if len(self.events):
			now = time.time()
			when, thread, args = self.events[0]
			max_timeout = min(max_timeout, max(when-now, 0))

		return max_timeout

class _event_poll(object):
	def __init__(self):
		self._info = {}

	def __nonzero__ (self):
		return len(self._info)

	def __len__(self):
		return len(self._info)

	def register(self, s, eventmask = DEFAULT_MASK):

		eventmask = s._wait_add(eventmask)
		self._info[s.fileno()] = (s, eventmask)

		self.ctl_add(s, eventmask)

	def unregister(self, s):

		eventmask = s._wait_del()
		if eventmask:
			self.ctl_mod(s, eventmask)
			self._info[s.fileno()] = (s, eventmask)
		else:
			self.ctl_del(s)
			del(self._info[s.fileno()])

	def poll(self, timeout = None):
		try:
			presult = self.wait(timeout)
		except (select.error, IOError), e:
			presult = []
			if e.args[0] != errno.EINTR: # Interrupted system call
				raise select.error, e

		cresult = []
		for fd, out_event in presult:
			s, tmp_event = self._info[fd]
			for thrd, in_event in s._waiting():
				if out_event & in_event or out_event & ERROR_MASK:
					cresult.append((s, thrd, in_event, out_event))

		return cresult

class event_poll(_event_poll):
	def __init__(self):
		super(event_poll, self).__init__()
		self._poll = select.poll()

	def ctl_add(self, fd, mask):
		self._poll.register(fd, mask)

	def ctl_mod(self, fd, mask):
		self._poll.register(fd, mask)

	def ctl_del(self, fd):
		self._poll.unregister(fd)

	def wait(self, timeout):
		return self._poll.poll(timeout)

if hasattr(select, "epoll"):
    class event_epoll(_event_poll):
        def __init__(self, size=-1):
            sef._size = size
            self._epoll = select.epoll()

        def ctl_add(self, fd, mask):
            self._epoll.register(fd, mask)

        def ctl_mod(self, fd, mask):
            self._epoll.modify(fd, mask)

        def ctl_del(self, fd):
            self._epoll.unregister(fd)

        def wait(self, timeout=None):
            timeout = timeout or -1
            return self._epoll.poll(timeout, self._size)

else:
    class event_epoll(_event_poll):
        def __init__(self, size = 32*1024):
            super(event_epoll, self).__init__()
            self._size  = size
            self._epoll = _epoll.epoll(self._size)

        def ctl_add(self, fd, mask):
            self._epoll._control(_epoll.CTL_ADD, fd.fileno(), mask)

        def ctl_mod(self, fd, mask):
            self._epoll._control(_epoll.CTL_MOD, fd.fileno(), mask)

        def ctl_del(self, fd):
            self._epoll._control(_epoll.CTL_DEL, fd.fileno(), 0)

        def wait(self, timeout):
            timeout = timeout is None and -1 or int(timeout)
            return self._epoll.wait(self._size, timeout)
#
# primary scheduling/event loop
#
the_event_list = event_list()
the_event_poll = event_poll()
the_loop_count = 0

stop = 0

def use_epoll(size = 32*1024):
	if hasattr(select, "epoll") or _epoll is not None:
		global the_event_poll
		the_event_poll = event_epoll(size)
		return True
	else:
		return False

def shutdown(countdown = 0):
	global stop
	stop = time.time() + countdown

def exit_handler(signum, frame):
	print 'Caught signal <%d> exiting.' % (signum)
	shutdown()

def reset_event_loop():
	#
	# dump all events/threads, I'm using this to shut down, so
	# if the event_loop exits, I want to startup another eventloop
	# thread set to perform shutdown functions.
	#
	global the_event_list
	global the_event_poll
	global the_loop_count
	global pending

	the_event_list = event_list()
	the_event_poll = event_poll()
	the_loop_count = 0

	pending = {}

	return None

def _exit_event_loop():

	if stop and not (stop > time.time()):
		return True
	else:
		return not (len(the_event_list) + len(the_event_poll) + len(pending))


def _real_event_loop(max_timeout):
	global the_loop_count

	while True:
		#
		# count number of times through the loop
		#
		the_loop_count += 1
		#
		# run scheduled coroutines
		#
		run_pending()
		#
		# check for exit ahead of potential sleep
		#
		if _exit_event_loop():
			break
		#
		# calculate max timeout to wait before resuming anything
		#
		delta = the_event_list.next_event(max_timeout)
		delta = delta * 1000 ## seconds to milliseconds
		#
		# wait on readiness events using calculated timeout
		#
		results = the_event_poll.poll(delta)
		#
		# once the wait/poll has completed, schedule timer events ahead
		# of readiness events. There can only be one resume per waiting
		# coro each iteration. The last schedule() in the iteration will
		# be the resumed event. The event_list must handle having a
		# sechedule() disappear.
		# 
		the_event_list.run_scheduled()

		for sock, thrd, in_event, out_event in results:
			schedule(thrd, out_event)
	#
	# exit...
	return None

def event_loop(max_timeout = 30.0):
	signal.signal(signal.SIGUSR1, exit_handler)
	global MAIN_EVENT_LOOP
	try:
		MAIN_EVENT_LOOP = greenlet(_real_event_loop)
		MAIN_EVENT_LOOP.switch(max_timeout)
	finally:
		preemptive_disable()
#
# end..
