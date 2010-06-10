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

"""httpload

A parallel URL fetching loop. Fetches a give URL repeatedly from multiple
coroutines, printing the elapsed wall clock time on completion.

"""

import coro
import corocurl

coro.socket_emulate()
corocurl.emulate()

import os
import sys
import time
import logging
import getopt
import exceptions
import pycurl

CONNECT_TIMEOUT = 30
DATA_TIMEOUT    = 120

class DataEater(object):
	def __init__(self):
		self.contents = ''

	def body_callback(self, buf):
		self.contents = self.contents + buf

	def clear(self):
		self.contents = ''

class RequestClient(coro.Thread):
	def __init__(self, *args, **kwargs):
		super(RequestClient, self).__init__(*args, **kwargs)

		self._fail  = 0
		self._good  = 0
		self._eater = DataEater()
		self._curl  = pycurl.Curl()

		self._curl.setopt(pycurl.CONNECTTIMEOUT, CONNECT_TIMEOUT)
		self._curl.setopt(pycurl.TIMEOUT,        DATA_TIMEOUT)
		self._curl.setopt(pycurl.WRITEFUNCTION,  self._eater.body_callback)
		self._curl.setopt(pycurl.HTTPHEADER,     kwargs['headers'])

	def run(self, id, url, requests, fifo):
		self._start = time.time()
		self._fifo  = fifo
		self._id    = id

		self._curl.setopt(pycurl.URL, url)

		while requests:
			self._eater.clear()

			try:
				self._curl.perform()
			except:
				self._fail += 1
			else:
				self._good += 1

			requests -= 1
		#
		# push results to server and exit
		#
		self._end = time.time()

		result = {
			'id':     self._id,
			'elapse': self._end - self._start,
			'fail':   self._fail,
			'good':   self._good}

		self._fifo.push(result)
		self._fifo = None

		return None

class RequestServer(coro.Thread):
	def init(self):
		self._fifo = coro.coroutine_fifo()
		self._data = []
		
	def run(self, url, threads, requests, headers):
		

		self.info('request server: %d threads %d requests each.' % (
			threads, requests))
		self._start = time.time()
		#
		# spawn N threads
		#
		for id in range(threads):
			client = RequestClient(
				args    = (id, url, requests, self._fifo),
				headers = headers)
			client.start()
		#
		# wait for client completions
		#
		while threads > len(self._data):
			self._data.append(self._fifo.pop())

		self._end = time.time()
		#
		# process results
		#
		fail = sum(map(lambda x: x['fail'], self._data))
		good = sum(map(lambda x: x['good'], self._data))

		self.info('request server: %d requests completed in %f seconds' % (
			threads * requests, self._end - self._start))
		self.info('request server: %d succeeded %d failed' % (good, fail))
		
		return None
#
#
#
def run(log, loglevel, url, threads, requests, headers):
	#
	# webserver and handler
	server = RequestServer(log = log, args=(url, threads, requests, headers))
	server.set_log_level(loglevel)
	server.start()
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

COMMAND_LINE_ARGS = ['help', 'url=', 'threads=', 'requests=', 'header=']

def usage(name, error = None):
	if error:
		print 'Error:', error
	print "  usage: %s [options]" % name
 
def main(argv, environ):
	progname = sys.argv[0]

	threads  = 1
	requests = 1
	loglevel = 'INFO'
	url      = None
	headers  = []

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
		elif field == '--threads':
			threads = int(val)
		elif field == '--requests':
			requests = int(val)
		elif field == '--header':
			headers.append(val)


	hndlr = logging.StreamHandler(sys.stdout)
	log   = coro.coroutine_logger('corohttpd')
	fmt   = logging.Formatter(LOG_FRMT)

	log.setLevel(logging.DEBUG)

	sys.stdout = coro.coroutine_stdout(log)
	sys.stderr = coro.coroutine_stderr(log)

	hndlr.setFormatter(fmt)
	log.addHandler(hndlr)
	loglevel = LOGLEVELS.get(loglevel, None)
	if loglevel is None:
		log.warn('Unknown logging level, using INFO: %r' % (loglevel, ))
		loglevel = logging.INFO

	if url:
		run(log, loglevel, url, threads, requests, headers)
	else:
		log.error('No URL given!')

	return None

if __name__ == '__main__':
	main(sys.argv, os.environ)

