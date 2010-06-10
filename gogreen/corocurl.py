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

"""corocurl

Emulation of PycURL objects which can be used inside the coro framework.

Written by Libor Michalek.
"""
import os
import getopt
import logging

import sys
import exceptions
import pycurl
import coro
import time
import select

__real_curl__ = pycurl.Curl
__real_mult__ = pycurl.CurlMulti

DEFAULT_YIELD_TIMEOUT = 2

class CoroutineCurlError (exceptions.Exception):
	pass

class coroutine_curl(object):
	'''coroutine_curl

	Emulation replacement for the standard pycurl.Curl() object. The
	object uses the non-blocking pycurl.CurlMulti interface to execute
	requests.
	'''

	def __init__(self):
		self._curl = __real_curl__()
		self._mult = __real_mult__()

		self._waits = {None: 0}
		self._fdno  = None

	def __getattr__(self, name):
		return getattr(pycurl, name)
	#
	# poll() registration API interface functions
	#
	def fileno(self):
		if self._fdno is None:
			raise CoroutineCurlError, 'coroutine curl no fd yet'

		return self._fdno

	def _wait_add(self, mask):
		self._waits[coro.current_thread()] = mask
		return reduce(lambda x, y: x|y, self._waits.values())

	def _wait_del(self):
		del(self._waits[coro.current_thread()])
		return reduce(lambda x, y: x|y, self._waits.values())

	def _waiting(self):
		return filter(lambda y: y[0] is not None, self._waits.items())
	#
	# internal
	#
	def _perform(self):
		current = coro.current_thread()
		if current is None:
			raise CoroutineCurlError, "coroutine curl in 'main'"

		while 1:
			ret = pycurl.E_CALL_MULTI_PERFORM

			while ret == pycurl.E_CALL_MULTI_PERFORM:
				ret, num = self._mult.perform()

			if not num:
				break
			#
			# build fdset and eventmask
			#
			read_set, send_set, excp_set = self._mult.fdset()
			if not read_set and not send_set:
				raise CoroutineCurlError, 'coroutine curl empty fdset'

			if read_set and send_set and read_set != send_set:
				raise CoroutineCurlError, 'coroutine curl bad fdset'

			self._fdno = (read_set + send_set)[0]
			eventmask  = 0

			if read_set:
				eventmask |= select.POLLIN

			if send_set:
				eventmask |= select.POLLOUT
			#
			# get timeout
			#
			# starting with pycurl version 7.16.0 the multi object
			# supplies the max yield timeout, otherwise we just use
			# a reasonable floor value.
			#
			if hasattr(self._mult, 'timeout'):
				timeout = self._mult.timeout()
			else:
				timeout = DEFAULT_YIELD_TIMEOUT
			#
			# wait
			#
			coro.the_event_poll.register(self, eventmask)
			result = current.Yield(timeout, 0)
			coro.the_event_poll.unregister(self)
			#
			# process results (result of 0 is a timeout)
			#
			self._fdno = None		

			if result is None:
				raise CoroutineSocketWake, 'socket has been awakened'

			if 0 < (result & coro.ERROR_MASK):
				raise pycurl.error, (socket.EBADF, 'Bad file descriptor')

		queued, success, failed = self._mult.info_read()
		if failed:
			raise pycurl.error, (failed[0][1], failed[0][2])
	#
	# emulated API
	#
	def perform(self):
		self._mult.add_handle(self._curl)
		try:
			self._perform()
		finally:
			self._mult.remove_handle(self._curl)

	def close(self):
		self._mult.close()
		self._curl.close()

	def errstr(self):
		return self._curl.errstr()

	def getinfo(self, option):
		return self._curl.getinfo(option)

	def setopt(self, option, value):
		return self._curl.setopt(option, value)

	def unsetopt(self, option):
		return self._curl.setopt(option)

class coroutine_multi(object):
	'''coroutine_multi

	coroutine replacement for the standard pycurl.CurlMulti() object.
	Since one should not need to deal with CurlMulti interface while
	using the coroutine framework, this remains unimplemented.
	'''

	def __init__(self):
		raise CoroutineCurlError, 'Are you sure you know what you are doing?'

def emulate():
	"replace some pycurl objects with coroutine aware objects"
	pycurl.Curl      = coroutine_curl
	pycurl.CurlMulti = coroutine_multi
	# sys.modules['pycurl'] = sys.modules[__name__]
#
# Standalone testing interface
#
TEST_CONNECT_TIMEOUT = 15
TEST_DATA_TIMEOUT    = 15

class CurlEater:
	def __init__(self):
		self.contents = ''

	def body_callback(self, buf):
		self.contents = self.contents + buf

class CurlFetch(coro.Thread):
	def run(self, url):
		self.info('starting fetch <%s>' % (url,))
		ce = CurlEater()

		c = pycurl.Curl()
		c.setopt(pycurl.URL, url)
		c.setopt(pycurl.CONNECTTIMEOUT, TEST_CONNECT_TIMEOUT)
		c.setopt(pycurl.TIMEOUT, TEST_DATA_TIMEOUT)
		c.setopt(pycurl.WRITEFUNCTION, ce.body_callback)

		try:
			c.perform()
		except:
			self.traceback()

		self.info('fetched %d bytes' % (len(ce.contents),))
		return None

def run(url, log, loglevel):
	#
	# turn on curl emulation
	emulate()
	#
	# webserver and handler
	fetch = CurlFetch(log = log, args = (url,))
	fetch.set_log_level(loglevel)
	fetch.start()
	#
	# primary event loop.
	coro.event_loop()
	#
	# never reached...
	return None

LOG_FRMT = '[%(name)s|%(coro)s|%(asctime)s|%(levelname)s] %(message)s'
LOGLEVELS = dict(
	CRITICAL=logging.CRITICAL, DEBUG=logging.DEBUG, ERROR=logging.ERROR,
	FATAL=logging.FATAL, INFO=logging.INFO, WARN=logging.WARN,
	WARNING=logging.WARNING)

COMMAND_LINE_ARGS = ['help', 'logfile=', 'loglevel=', 'url=']

def usage(name, error = None):
	if error:
		print 'Error:', error
	print "  usage: %s [options]" % name
 
def main(argv, environ):
	progname = sys.argv[0]

	url      = None
	logfile  = None
	loglevel = 'INFO'

	dirname  = os.path.dirname(os.path.abspath(progname))
	os.chdir(dirname)

	try:
		list, args = getopt.getopt(argv[1:], [], COMMAND_LINE_ARGS)
	except getopt.error, why:
		usage(progname, why)
		return None

	for (field, val) in list:
		if field == '--help':
			usage(progname)
			return None
		elif field == '--url':
			url = val
		elif field == '--logfile':
			logfile = val
		elif field == '--loglevel':
			loglevel = val
	#
	# setup logging
	#
	hndlr = logging.StreamHandler(sys.stdout)
	log = coro.coroutine_logger('corocurl')
	fmt = logging.Formatter(LOG_FRMT)

	log.setLevel(logging.DEBUG)

	sys.stdout = coro.coroutine_stdout(log)
	sys.stderr = coro.coroutine_stderr(log)

	hndlr.setFormatter(fmt)
	log.addHandler(hndlr)
	loglevel = LOGLEVELS.get(loglevel, None)
	if loglevel is None:
		log.warn('Unknown logging level, using INFO: %r' % (loglevel, ))
		loglevel = logging.INFO
	#
	# check param and execute
	#
	if url is not None:
		result = run(url, log, loglevel)
	else:
		log.error('no url provided!')
		result = None

	return result

if __name__ == '__main__':
	main(sys.argv, os.environ)


#
# end...
