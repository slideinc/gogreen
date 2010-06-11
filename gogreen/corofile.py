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

"""corofile

Emulation of file objects with nonblocking semantics. Useful
for handling standard io.

Written by Donovan Preston.
"""

import coro

import os
import sys
import fcntl
import errno
import select

ERROR_MASK = select.POLLERR|select.POLLHUP|select.POLLNVAL
BUFFER_CHUNK_SIZE = 32*1024

class NonblockingFile(object):
	def __init__(self, fp):
		self._fp = fp

		F = fcntl.fcntl(self._fp.fileno(), fcntl.F_GETFL)
		F = F | os.O_NONBLOCK
		fcntl.fcntl(self._fp.fileno(), fcntl.F_SETFL, F)

		self._chunk = BUFFER_CHUNK_SIZE
		self._data  = ''
		self._waits = {None: 0}

	def fileno(self):
		return self._fp.fileno()

	def _wait_add(self, mask):
		self._waits[coro.current_thread()] = mask
		return reduce(lambda x, y: x|y, self._waits.values())

	def _wait_del(self):
		del(self._waits[coro.current_thread()])
		return reduce(lambda x, y: x|y, self._waits.values())

	def _waiting(self):
		return filter(lambda y: y[0] is not None, self._waits.items())

	def _wait_for_event(self, eventmask):
		me = coro.current_thread()
		if me is None:
			raise coro.CoroutineThreadError, "coroutine sockets in 'main'"

		coro.the_event_poll.register(self, eventmask)
		result = me.Yield()
		coro.the_event_poll.unregister(self)

		if result is None:
			raise coro.CoroutineSocketWake, 'file descriptor has been awakened'

		if result & eventmask:
			return None

		if result & ERROR_MASK:
			raise IOError(errno.EPIPE, 'Broken pipe')
		#
		# all cases should have been handled by this point
		return None

	def read(self, numbytes = -1):
		while numbytes < 0 or numbytes > len(self._data):
			self._wait_for_event(select.POLLIN|select.POLLHUP)
			
			read = os.read(self.fileno(), self._chunk)
			if not read:
				break

			self._data += read

		if numbytes < 0:
			result, self._data = self._data, ''
		else:
			result, self._data = self._data[:numbytes], self._data[numbytes:]

		return result

	def write(self, data):
		pos = 0

		while pos < len(data):
			self._wait_for_event(select.POLLOUT)

			size = os.write(self.fileno(), data[pos:pos + self._chunk])
			pos += size

	def flush(self):
		if hasattr(self._fp, 'flush'):
			self._wait_for_event(select.POLLOUT)
			self._fp.flush()
		
	def close(self):
		self._fp.close()


__old_popen2__ = os.popen2

def popen2(*args, **kw):
	stdin, stdout = __old_popen2__(*args, **kw)
	return NonblockingFile(stdin), NonblockingFile(stdout)


def emulate_popen2():
	if os.popen2 is not popen2:
		os.popen2 = popen2

__old_popen3__ = os.popen3

def popen3(*args, **kwargs):
	stdin, stdout, stderr = __old_popen3__(*args, **kwargs)
	return NonblockingFile(stdin), NonblockingFile(stdout), NonblockingFile(stderr)

def install_stdio():
	sys.stdin = NonblockingFile(sys.stdin)
	sys.stdout = NonblockingFile(sys.stdout)
	return sys.stdin, sys.stdout

def echostdin():
	sin, sout = install_stdio()
	
	sout.write("HELLO WORLD!\n")
	echo = ''
	while True:
		char = sin.read(1)
		if not char:
			return
		if char == '\n':
			sout.write('%s\n' % (echo, ))
			echo = ''
		else:
			echo += char


def readToEOF():
	sin, sout = install_stdio()

	read = sin.read()
	sout.write(read)


if __name__ == '__main__':
	coro.spawn(echostdin)
#	coro.spawn(readToEOF)
	coro.event_loop()


