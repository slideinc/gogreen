#!/usr/bin/env python
# -*- Mode: Python; tab-width: 4 -*-

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

'''fileobject

emulate a fileobject using a seperate process to handle IO for coroutine
concurrency

Written by Libor Michalek. 2006
'''

import coro
import corofile
import coroqueue

# corofile.emulate_popen2()

import os
import sys
import struct
import exceptions
import pickle
import signal
import weakref
import signal

STAUS_OPENED = 1
STAUS_CLOSED = 0

COMMAND_SIZE   = 8 # cmd is 8 bytes, four for identifier and four for length
COMMAND_OPEN   = 0
COMMAND_CLOSE  = 1
COMMAND_READ   = 2
COMMAND_WRITE  = 3
COMMAND_FLUSH  = 4
COMMAND_SEEK   = 5

COMMAND_STAT   = 6
COMMAND_MKDIRS = 7
COMMAND_CHMOD  = 8
COMMAND_UNLINK = 9
COMMAND_UTIME  = 10
COMMAND_TELL   = 11
COMMAND_READLN = 12

COMMAND_MAP = {
	COMMAND_OPEN:   'open',
	COMMAND_CLOSE:  'close',
	COMMAND_READ:   'read',
	COMMAND_WRITE:  'write',
	COMMAND_FLUSH:  'flush',
	COMMAND_SEEK:   'seek',
	COMMAND_STAT:   'stat',
	COMMAND_MKDIRS: 'makedirs',
	COMMAND_CHMOD:  'chmod',
	COMMAND_UNLINK: 'unlink',
	COMMAND_UTIME:  'utime',
	COMMAND_TELL:   'tell',
	COMMAND_READLN: 'readline',
	}

RESPONSE_SUCCESS = 100
RESPONSE_ERROR   = 101

BUFFER_READ_SIZE  =      32*1024
BUFFER_WRITE_SIZE = 16*1024*1024

DEFAULT_QUEUE_SIZE = 16

NOWAIT = False

class CoroFileObjectError (exceptions.Exception):
	pass

class CoroFileObject(object):
	def __init__(self):
		self._stdi, self._stdo = os.popen2([
			os.path.realpath(__file__.replace('.pyc', '.py'))])

		self._data = ''
		self._status = STAUS_CLOSED
		self.name = None

	def __del__(self):
		if self._status == STAUS_OPENED:
			self.close(return_to_queue = False)

		if self._stdi: self._stdi.close()
		if self._stdo: self._stdo.close()
		if not NOWAIT: os.wait()

	def _cmd_(self, cmd, data = ''):
		self._stdi.write(struct.pack('!ii', cmd, len(data)))
		self._stdi.write(data)
		self._stdi.flush()

		data = self._stdo.read(COMMAND_SIZE)
		if not data:
			raise CoroFileObjectError('No command from child')

		response, size = struct.unpack('!ii', data)
		if size:
			data = self._stdo.read(size)
		else:
			data = None

		if response == RESPONSE_ERROR:
			raise pickle.loads(data)
		else:
			return data

	def open(self, name, mode = 'r'):
		if self._status == STAUS_OPENED:
			self.close()

		result = self._cmd_(COMMAND_OPEN, '%s\n%s' % (name, mode))
		self._status = STAUS_OPENED
		self.name = name
		return result

	def read(self, bytes = -1):
		while bytes < 0 or bytes > len(self._data):
			result = self._cmd_(COMMAND_READ)
			if result is None:
				break

			self._data += result
		#
		# either have all the data we want or file EOF
		#
		if bytes < 0:
			# Return all remaining data
			data = self._data
			self._data = ''
		else:
			data       = self._data[:bytes]
			self._data = self._data[bytes:]

		return data

	def readline(self, bytes = -1):
		while bytes < 0 or bytes > len(self._data):
			result = self._cmd_(COMMAND_READLN)
			if result is None:
				break
			self._data += result
			if result[-1] == '\n':
				break

		#
		# either have all the data we want or file EOF
		#
		if bytes < 0:
			# Return all remaining data
			data = self._data
			self._data = ''
		else:
			data       = self._data[:bytes]
			self._data = self._data[bytes:]

		return data			
		
	
	def write(self, data):
		while data:
			self._cmd_(COMMAND_WRITE, data[:BUFFER_WRITE_SIZE])
			data = data[BUFFER_WRITE_SIZE:]

	def seek(self, offset):
		self._data = ''
		return self._cmd_(COMMAND_SEEK, struct.pack('!i', offset))

	def tell(self):
		return int(self._cmd_(COMMAND_TELL))

	def flush(self):
		return self._cmd_(COMMAND_FLUSH)

	def close(self, return_to_queue = True):
		"""close closes this fileobject
		NB: The return_to_queue parameter is ignored.  It is required
		for interface compatability with the AutoCleanFileOjbect subclass.
		"""
		self._data = ''
		self._status = STAUS_CLOSED
		return self._cmd_(COMMAND_CLOSE)
	#
	# non-standard extensions
	#
	def stat(self, path):
		args = eval(self._cmd_(COMMAND_STAT, path))
		return os._make_stat_result(*args)

	def makedirs(self, path):
		return eval(self._cmd_(COMMAND_MKDIRS, path))

	def chmod(self, path, mode):
		return eval(self._cmd_(COMMAND_CHMOD, '%s\n%d' % (path, mode)))

	def unlink(self, path):
		return eval(self._cmd_(COMMAND_UNLINK, path))

	def utime(self, path, value = None):
		return eval(self._cmd_(COMMAND_UTIME, '%s\n%s' % (path, str(value))))


class AutoCleanFileObject(CoroFileObject):
	"""AutoCleanFileOjbect overrides close to optionally return itself
	to the filequeue after closing.
	"""
	def close(self, return_to_queue = True):
		"""close closes this fileobject

		return_to_queue: return this object back to the filequeue if
			True, the default.
		"""
		res = super(AutoCleanFileObject, self).close()
		if return_to_queue:
			filequeue.put(self)
		return res

class FileObjectHandler(object):
	def __init__(self, stdin, stdout):
		self._stdi = stdin
		self._stdo = stdout
		self._fd   = None

	def open(self, data):
		name, mode = data.split('\n')

		self._fd = file(name, mode, BUFFER_READ_SIZE)

	def close(self, data):
		self._fd.close()

	def read(self, data):
		return self._fd.read(BUFFER_READ_SIZE)

	def readline(self, data):
		r = self._fd.readline(BUFFER_READ_SIZE)
		return r

	def write(self, data):
		return self._fd.write(data)

	def flush(self, data):
		return self._fd.flush()

	def seek(self, data):
		(offset,) = struct.unpack('!i', data)
		return self._fd.seek(offset)

	def tell(self, data):
		return str(self._fd.tell())
	#
	# non-standard extensions
	#
	def stat(self, data):
		return str(os.stat(data).__reduce__()[1])

	def makedirs(self, data):
		return str(os.makedirs(data))

	def chmod(self, data):
		path, mode = data.split('\n')
		return str(os.chmod(path, int(mode)))

	def unlink(self, data):
		return str(os.unlink(data))

	def utime(self, data):
		path, value = data.split('\n')
		return str(os.utime(path, eval(value)))

	def run(self):
		result = 0

		while True:
			try:
				data = self._stdi.read(COMMAND_SIZE)
			except KeyboardInterrupt:
				data = None

			if not data:
				result = 1
				break

			cmd, size = struct.unpack('!ii', data)
			if size:
				data = self._stdi.read(size)
			else:
				data = ''

			if size != len(data):
				result = 2
				break

			handler = getattr(self, COMMAND_MAP.get(cmd, 'none'), None)
			if handler is None:
				result = 3
				break

			try:
				result = handler(data)
			except exceptions.Exception, e:
				result   = pickle.dumps(e)
				response = RESPONSE_ERROR
			else:
				response = RESPONSE_SUCCESS

			if result is None:
				result = ''

			try:
				self._stdo.write(struct.pack('!ii', response, len(result)))
				self._stdo.write(result)
				self._stdo.flush()
			except IOError:
				result = 4
				break

		return result

class CoroFileQueue(coroqueue.Queue):
	def __init__(self, size, timeout = None):
		super(CoroFileQueue, self).__init__(
			AutoCleanFileObject, (), {}, size = size, timeout = timeout)

		self._fd_save = {}
		self._fd_refs  = {}

	def _save_info(self, o):
		def dropped(ref):
			self._fd_save.pop(self._fd_refs.pop(id(ref), None), None)

		p = weakref.proxy(o, dropped)
		self._fd_save[id(o)] = p
		self._fd_refs[id(p)] = id(o)

		return super(CoroFileQueue, self)._save_info(o)

	def _drop_info(self, o):
		self._fd_refs.pop(id(self._fd_save.pop(id(o), None)), None)

		return super(CoroFileQueue, self)._drop_info(o)

	def outstanding(self):
		return map(lambda i: getattr(i, 'name', None), self._fd_save.values())

	
filequeue = CoroFileQueue(DEFAULT_QUEUE_SIZE)

def resize(size):
	return filequeue.resize(size)

def size():
	return filequeue.size()

def _fo_open(name, mode = 'r'):
	fd = filequeue.get()
	if fd is None:
		return None

	try:
		fd.open(name, mode)
	except:
		filequeue.put(fd)
		raise
	return fd

#
# TODO: Remove close method once all references to it have been purged.
#
def _fo_close(fd):
	fd.close(return_to_queue=False)
	return filequeue.put(fd)

def __command__(name, *args, **kwargs):
	fd = filequeue.get()
	if fd is None:
		return None

	try:
		return getattr(fd, name)(*args, **kwargs)
	finally:
		filequeue.put(fd)

def _fo_stat(path):
	return __command__('stat', path)

def _fo_makedirs(path):
	return __command__('makedirs', path)

def _fo_chmod(path, mode):
	return __command__('chmod', path, mode)

def _fo_unlink(path):
	return __command__('unlink', path)

def _fo_utime(path, value = None):
	return __command__('utime', path, value = value)

#
# os.* calls
#
stat = os.stat
makedirs = os.makedirs
chmod = os.chmod
unlink = os.unlink
utime = os.utime

#
# file/open call
#
open = file
#
# close call
#
def dummy_close(fd):
	return fd.close()

close = dummy_close

# a generator function for doing readlines in a fileobject friendly manner
def iterlines(fd):
	while True:
		ln = fd.readline()
		if not ln:
			break
		yield ln

def iterfiles(filenames):
	for fn in filenames:
		fd = open(fn)
		for ln in iterlines(fd):
			yield ln
		fd.close()

def emulate():
	fileobject = sys.modules['gogreen.fileobject']
	#
	# os.* calls
	#
	fileobject.stat = _fo_stat
	fileobject.makedirs = _fo_makedirs
	fileobject.chmod = _fo_chmod
	fileobject.unlink = _fo_unlink
	fileobject.utime = _fo_utime

	#
	# file/open call
	#
	fileobject.open  = _fo_open
	fileobject.close = _fo_close

def nowait():
	'''nowait

	NOTE: GLOBAL SIGNAL CHANGE!

	Do not wait for the terminated/exiting fileobject, since this can
	block. To prevent the processes from becoming unreaped zombies we
	disable the SIGCHILD signal. (see man wait(2))
	'''
	global NOWAIT
	NOWAIT = True

	signal.signal(signal.SIGCHLD, signal.SIG_IGN)

	
if __name__ == '__main__':
	import prctl

	prctl.prctl(prctl.PDEATHSIG, signal.SIGTERM)

	handler = FileObjectHandler(sys.stdin, sys.stdout)
	value   = handler.run()
	sys.exit(value)
#
# end..
