#!/usr/bin/env python
# -*- Mode: Python; tab-width: 4 -*-

# Copyright (c) 1999 eGroups, Inc.
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

"""
corohttpd
	This is an infrastructure for having a http server using coroutines
	There are three major classes defined here:
	HttpProtocol
		This is a descendent of coro.Thread. It handles the connection
		to the client, spawned by HttpServer. Its run method goes through
		the stages of reading the request, filling out a HttpRequest and
		finding the right handler, etc. It is separate from the HttpRequest
		object because each HttpProtocol, which represents one socket,
		can spawn multiple request with socket keepalives.
	HttpRequest
		This object collects all of the data for a request. It is initialized
		from the HttpClient thread with the http request data, and is then
		passed to the handler to receive data. It attempts to enforce a valid
		http protocol on the response
	HttpServer
		This is a thread which just sits accepting on a socket, and spawning
		HttpProtocols to handle incoming requests

	Additionally, the server expects http handler classes which respond
	to match and handle_request. There is an example class,
	HttpFileHandler, which is a basic handler to respond to GET requests
	to a document root. It will return any file which exists.

	To use, implement your own handler class which responds to match and
	handle_request. Then, create a server, add handlers to the server,
	and start it. You then need to call the event_loop yourself.
	Something like:

		server = HttpServer(args = (('0.0.0.0', 7001), 'access.log'))
		file_handler = HttpFileHandler('/home/htdocs/')
		server.push_handler(file_handler)
		server.start()
		coro.event_loop(30.0)
"""

import os
import coro
import socket
import string
import sys
import time
import re
import bisect
import errno
import logging
import logging.handlers
import getopt
import exceptions
import tempfile
import cStringIO
import urllib
import cgi
import BaseHTTPServer
import inspect
import Cookie
import zlib
import struct

import backdoor
import statistics

coro.socket_emulate()

SAVE_REQUEST_DEPTH  = 100
REQUEST_COUNT_DEPTH = 900
REQUEST_STATS_PERIOD = [15, 60, 300, 600]
ACCESS_LOG_SIZE_MAX  = 64*1024*1024
ACCESS_LOG_COUNT_MAX = 128

READ_CHUNK_SIZE = 32*1024
POST_READ_CHUNK_SIZE = 64*1024

SEND_BUF_SIZE = 128*1024
RECV_BUF_SIZE = 128*1024

HEADER_CLIENT_IPS = ['True-Client-IP', 'NS-Client-IP']

FUTURAMA = 'Mon, 28-Sep-2026 21:46:59 GMT'

try:
	# If we can get the hostname, obfuscate and add a header
	hostre = re.compile('^([A-Za-z\-_]+)(\d+|)(\.\w+|)')
	hostre = hostre.search(socket.getfqdn())
	hostgp = hostre and hostre.groups() or ('unknown','X','unknown')
	HOST_HEADER = (
		'X-Host',
		hostgp[0][0] + hostgp[0][-1] + hostgp[1] + hostgp[2])
except:
	HOST_HEADER = ('X-Host', 'FAIL')

def save_n(queue, value, data, depth):
	if value > queue[0][0]:
		bisect.insort(queue, (value, data))
		while len(queue) > depth:
			del(queue[0])

def header_blackcheck(rules, headers):
	for header, rule in rules:
		header = headers.get(header, [])
		header = (isinstance(header, str) and [[header]] or [header])[0]

		if not header:
			return True

		for element in header:
			if rule(element):
				return True

	return False

def gzip_stream(s):
	header = struct.pack(
		'<BBBBIBB',
		0x1f,
		0x8b,
		0x08,
		0x00,
		int(time.time()),
		0x00,
		0x03)

	size = len(s)
	crc  = zlib.crc32(s)

	return header + zlib.compress(s)[2:-4] + struct.pack('<II', crc, size)

def deflate_stream(s):
	return zlib.compress(s)

SUPPORTED_ENCODINGS = [('gzip', gzip_stream), ('deflate', deflate_stream)]

class ConnectionClosed(Exception):
	def __repr__(self):
		return "ConnectionClosed(%r)" % (self.args[0], )

NO_REQUEST_YET = "<no request yet>"
NO_COMMAND_YET = "<no command yet>"
BETWEEN_REQUESTS = "<between requests>"

class HttpAllow(object):
	'''HttpAllow

	Access check based on IP address. Initialized with a list of IP
	addresses, using an optional netmask, that are allowed to access
	the resource. An IP is checked against the list uring the match
	method.
	'''
	def __init__(self, allow):
		self._allow = []

		for address in allow:
			address = address.split('/')

			if 1 < len(address):
				mask = int(address[-1])
			else:
				mask = 32

			address = reduce(
				lambda x, y: (x<<8) | y,
				map(lambda i: int(i), address[0].split('.')))

			mask = (1 << (32 - mask)) - 1

			self._allow.append({'addr': address, 'mask': mask})

	def match(self, address):
		address = reduce(
			lambda x, y: (x<<8)|y,
			map(lambda i: int(i), address.split('.')))

		for allow in self._allow:
			if allow['addr']|allow['mask'] == address|allow['mask']:
				return True

		return False


class HttpProtocol(coro.Thread, BaseHTTPServer.BaseHTTPRequestHandler):
	protocol_version = 'HTTP/1.1'
	server_version = 'corohttpd/0.2'
	request_version = 'HTTP/0.9'

	connection = None
	client_address = ('<no address yet>', 0)
	close_connection = 0
	server = None
	request = None
	handlers = []
	buffer = ''
	_index = -1
	closed = False
	_chunked = False
	requestline = NO_REQUEST_YET
	command = NO_COMMAND_YET
	_reply_code = 200
	_request_count = 0

	def __init__(self, *args, **kwargs):
		super(HttpProtocol, self).__init__(*args, **kwargs)
		#
		# DO NOT call the BaseHTTPRequestHandler __init__. It kicks
		# off the request handling immediately. We need it to happen
		# in run instead. Since the base class for BaseHTTPRequestHandler
		# (SocketServer.BaseRequestHandler) is not a subclass of object,
		# the super call will not invoke the __init__ handler for it,
		# only for coro.Thread.
		#
		self._tbrec = kwargs.get('tbrec', None)
		self._debug_read = kwargs.get('debug_read', False)

		self._rsize = 0
		self._wsize = 0

		self._debug_read_buffers = []

		self._default_headers = []
		self._reply_headers = {}
		self._encblack = None

		self.accumulator = None
		self.headers = {}

		self.raw_requestline = ''
		self._push_time = 0
		self._req_time = 0
		self._sent_headers = False
		self._encode_write = False
		self._encode_wrote = False
		self._old_loglevel = None

	def run(self, conn, client_address, server, handlers):
		## TODO get rid of _conn and use request instead
		## same with these other two
		self.connection = conn
		self.client_address = client_address
		self.server = server
		self.handlers = handlers

		self.rfile = self

		self.handle()

		return None

	def complete(self):
		self.server.record_counts(self._request_count)
		self.server = None

		try:
			self.connection.shutdown(2)
		except socket.error:
			pass
		self.connection = None

		self.closed = True
		self.client_address = ('<no address>', 0)
		self.handlers = []
		self.buffer = ''
		self._index = -1
		self.requestline = NO_REQUEST_YET
		self.headers = None
		self.rfile = None

	def handle_one_request(self):
		self.raw_requestline = ''
		self._push_time = 0
		self._req_time = 0
		self._rsize = 0
		self._wsize = 0
		self._sent_headers = False
		self._encode_write = False
		self._encode_wrote = False
		self._reply_headers = {}
		self._reply_code = 200

		try:
			self.really_handle()

			if self._chunked:
				self.write('0\r\n\r\n')

			if not self.close_connection:
				self.requestline = BETWEEN_REQUESTS

			return None
		except ConnectionClosed, e:
			self.warn('connection terminated: %r' % (e,))
		except socket.error, e:
			if e[0] in [errno.EBADF, errno.ECONNRESET, errno.EPIPE]:
				self.debug('socket error: %r' % (e.args,))
			else:
				self.warn('socket error: %r' % (e.args,))
		except coro.TimeoutError, e:
			if self.raw_requestline:
				self.warn('Timeout: %r for %r' % (
					e.args[0], self.client_address))
		except coro.CoroutineSocketWake:
			pass
		except:
			self.traceback()
		#
		# exception cases fall through.
		#
		self.close_connection = 1

	def really_handle(self):
		#
		# get request line and start timer
		#
		self.raw_requestline = self.readline()
		self._req_time = time.time()
		self.clear()

		if not self.raw_requestline:
			self.close_connection = 1
			return

		if not self.parse_request():
			self.close_connection = 1
			return

		keep_alive = self.headers.get('Keep-Alive', None)
		if keep_alive is not None:
			try:
				self.connection.settimeout(int(keep_alive))
			except ValueError:
				## not an int; do nothing
				pass

		self.debug('from: %r request: %r' % (
			self.client_address, self.requestline,))

		for key, value in self._default_headers:
			self.set_header(key, value)

		self.request = HttpRequest(
			self, self.requestline, self.command, self.path, self.headers)
		self.server.request_started(self.request, self._req_time)

		try:
			try:
				for handler in self.handlers:
					if handler.match(self.request):
						self.debug('Calling handler: %r' % (handler,))

						handler.handle_request(self.request)
						self.push('')
						break
				else:
					self.debug('handler not found: %r' % (self.request))
					self.send_error(404)
			except (
				ConnectionClosed,
				coro.TimeoutError,
				coro.CoroutineSocketWake,
				socket.error):
				#
				# can not send the error, since it is an IO problem,
				# but set the response code anyway to something denoting
				# the issue
				#
				self.traceback(logging.DEBUG)
				self.response(506)
				raise
			except:
				self.traceback()
				self.send_error(500)
		finally:
			self.server.request_ended(
				self.request,
				self._reply_code,
				self._req_time,
				self._push_time,
				self._rsize,
				self._wsize)

			if self._debug_read:
				self.log_reads()
				self._debug_read = False
				self._debug_read_buffers = []

			if self._old_loglevel is not None:
				self.set_log_level(self._old_loglevel)
				self._old_loglevel = None

			self._request_count += 1
			self.raw_requestline = ''
			self.request = None
			self.accumulator = None

		return None

	def send_error(self, code, message=None):
		self.response(code)
		self.set_header('content-type', 'text/html')
		self.set_header('connection', 'close')

		if (
			self.command != 'HEAD' and code >= 200
			and code not in (204, 304)):
			if message is None:
				message = self.responses[code][0]
			message = message.replace(
				"&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
			explain = self.responses[code][1]
			content = self.error_message_format % dict(
				code=code, message=message, explain=explain)
			self.set_header('Content-Length', len(content))
			self.push(content)
		else:
			self.push('')

	def log_request(self, code='-', size='-'):
		"""log_request
		
		Called by BaseHTTPServer.HTTPServer to log the request completion.

		There is not enough information here to properly log the request;
		so we just ignore this and write to the access log ourselves.
		"""
		pass

   	def log_error(self, format, *args):
		"""log_error

		Called by BaseHTTPServer.HTTPServer to log an error.
		"""
		formatted = format % args
		self.error(formatted)
		self.info('Request: %s' % (self.requestline, ))
		if '404' not in formatted:
			for key, value in self.headers.items():
				self.info('Header: %s: %s' % (key, value ))

	def log_message(self, format, *args):
		"""log_message
	
		Called by BaseHTTPServer.HTTPServer to log a message.
		"""
		self.info(format % args)

	def log_reads(self):
		"""log_reads

		Write the contents of _debug_read_buffers out to the log.
		"""
		self.debug('----------BEGIN DEBUG REPORT----------')
		for data in self._debug_read_buffers:
			if isinstance(data, (str, unicode)):
				self.debug(data)
			else:
				self.debug(repr(data))
		self.debug('---------- END DEBUG REPORT ----------')

	def set_debug_read(self, flag=True):
		"""set_debug_read

		Call to set (or unset) the _debug_read flag.  If this flag is
		set, data received in calls to read and readlines will be
		logged.
		"""
		self._debug_read = flag

	def add_debug_read_data(self, s):
		self._debug_read_buffers.append(s)

	def req_log_level(self, level):
		'''req_log_level

		Set the coroutine log level for one request.
		'''
		self._old_loglevel = self.get_log_level()
		self.set_log_level(level)

	def set_encode_blacklist(self, data):
		'''set_encode_blacklist

		Set the response encoding blacklist. (added by server which
		maintains master list)

		NOTE: the default is no blacklist, which means that no encoding
		      will be performed. To encode all responses, push an empty
			  blacklist.
		'''
		self._encblack = data

	def address_string(self):
		"""address_string

		Called by BaseHTTPServer.HTTPServer to get the address of the
		remote client host to put in the access log.
		"""
		if self.request:
			for header_name in HEADER_CLIENT_IPS:
				ip = self.request.get_header(header_name)
				if ip: break
		else:
			ip = None

		if ip is None:
			return str(self.client_address[0])
		else:
			return ip

	# This is for handlers that process PUT/POST themselves.
	# This whole thing needs to be redone with a file-like
	# interface to 'stdin' for requests, and we need to think
	# about HTTP/1.1 and pipelining, etc...

	def read(self, size):
		while len(self.buffer) < size:
			buffer = self.connection.recv(READ_CHUNK_SIZE)
			if not buffer:
				raise ConnectionClosed("Remote host closed connection in read")
			self.buffer += buffer
			self._rsize += len(buffer)

			if self.accumulator:
				self.accumulator.recv_bytes(len(buffer))

		result = self.buffer[:size]
		self.buffer = self.buffer[size:]

		if self._debug_read: self._debug_read_buffers.append(result)
		return result

	def readline(self, size=None):
		while 0 > self._index:
			buffer = self.connection.recv(READ_CHUNK_SIZE)
			if not buffer:
				return buffer

			self.buffer += buffer
			self._rsize += len(buffer)
			self._index = string.find(self.buffer, '\n')

			if self.accumulator:
				self.accumulator.recv_bytes(len(buffer))

		result = self.buffer[:self._index+1]

		self.buffer = self.buffer[self._index + 1:]
		self._index = string.find(self.buffer, '\n')

		if self._debug_read: self._debug_read_buffers.append(result)
		return result

	def write(self, data):
		olb = lb = len(data)
		while lb:
			ns = self.connection.send(data[olb-lb:])
			if self.accumulator:
				self.accumulator.sent_bytes(ns)
			lb = lb - ns
		self._wsize = self._wsize + olb
		return olb

	def set_default_headers(self, data):
		self._default_headers = data

	def set_header(self, key, value, overwrite = True):
		value = str(value)
		if key.lower() == 'connection' and value.lower() == 'close':
			self.close_connection = 1

		if overwrite:
			self._reply_headers[key.lower()] = [value]
		else:
			self._reply_headers.setdefault(key.lower(), []).append(value)

	def get_outgoing_header(self, key, default = None):
		return self._reply_headers.get(key.lower(), default)

	def has_outgoing_header(self, key):
		return self._reply_headers.has_key(key.lower())

	def pop_outgoing_header(self, key):
		return self._reply_headers.pop(key.lower(), None)

	def response(self, code=200):
		self._reply_code = code

	def encode(self, s):
		#
		# check if encoding is allowed locally.
		#
		if not s:
			return s
		#
		# code path encode selection.
		if not self._encode_write:
			return s
		#
		# encoding capability configuration 
		if self._encblack is None:
			return s
		
		ingress, egress = self._encblack
		#
		# egress/content blacklist
		if header_blackcheck(egress, self._reply_headers):
			return s
		#
		# generate Vary header before checking the encode header or the 
		# ingress blacklist, since that will not effect that this content
		# MAY be encoded. From this point on an encode is possible and
		# will depend on what the client has sent.
		# 
		vary = map(lambda i: i[0], ingress)
		vary.append('accept-encoding')
		vary = map(lambda i: '-'.join(map(str.title, i.split('-'))), vary)

		self.set_header('Vary', ','.join(vary))
		#
		# decode accept-encoding header
		#
		header = self.headers.get('accept-encoding', None)
		if not header:
			return s

		encodings = []
		for node in map(lambda i: i.split(';'), header.split(',')):
			if len(node) < 2:
				encodings.append((node[0], 1))
				continue

			node, quality = node[:2]
			try:
				quality = float(quality.split('=')[1])
			except (ValueError, IndexError), e:
				continue

			encodings.append((node, quality))

		encodings = filter(lambda i: i[1], encodings)
		encodings.sort(key = lambda i: i[1], reverse = True)
		encodings = set(map(lambda i: i[0].strip(), encodings))

		if not encodings:
			return s
		#
		# check the headers against supported types.
		#
		for ename, efunc in SUPPORTED_ENCODINGS:
			if ename in encodings:
			   break
		else:
			ename, efunc = None, None

		if ename is None:
			return s
		#
		# ingress header check
		if header_blackcheck(ingress, self.headers):
			return s

		#
		# compress
		#
		s = efunc(s)
		#
		# generate encoding specific headers.
		#
		self.set_header('Content-Encoding', ename)
		self.set_header('Content-Length', len(s))

		self._encode_wrote = True
		return s
	
	def push(self, s, encode = False):
		self._push_time = time.time()
		#
		# toggle encode, once a push is encoded it needs to stay
		#
		self._encode_write |= encode

		if self._encode_wrote and self._sent_headers and s:
			raise RuntimeError('Cannot encode after headers have been sent')

		if self.request_version == 'HTTP/0.9' or self._sent_headers:
			return self.write(s)

		if self.close_connection:
			self.set_header('connection', 'close', overwrite = True)
		if not self.has_outgoing_header('server'):
			self.set_header('server', self.version_string())
		if not self.has_outgoing_header('date'):
			self.set_header('date', self.date_time_string())
		if not self.has_outgoing_header('connection'):
			self.set_header('connection', self.headers.get(
				'connection', 'close').strip())

		transfer = self.get_outgoing_header('transfer-encoding', [])
		if transfer:
			self._chunked = transfer[-1] == 'chunked'
		else:
			self._chunked = False

		if self._encode_write and self._chunked:
			raise RuntimeError('HTTP encode with chunk unsupported')

		s = self.encode(s)

		if not self._chunked and not self.has_outgoing_header('content-length'):
			self.set_header('content-length', len(s))

		keep_alive = self.get_outgoing_header(
			'connection', ['close'])[-1].lower()

		if keep_alive == 'keep-alive':
			self.close_connection = 0
		else:
			self.close_connection = 1

		headers = []
		for key, values in self._reply_headers.items():
			for value in values:
				headers.append(
					'%s: %s' % (
					'-'.join(map(str.title, key.split('-'))), value))

		headers.extend(('', ''))
		self._sent_headers = True

		return self.write(
			"%(version)s %(code)s %(message)s\r\n"
			"%(headers)s%(body)s" % dict(
				version=self.protocol_version,
				code=self._reply_code,
				message=self.responses[self._reply_code][0],
				headers='\r\n'.join(headers),
				body=s))

	def push_chunked(self, stuff):
		chunked = '%X\r\n%s\r\n' % (len(stuff), stuff)
		if self._sent_headers:
			self.write(chunked)
		else:
			self.set_header('transfer-encoding', 'chunked')
			self.push(chunked)

	def shutdown(self, nice = False):
		self.close_connection = 1

		if not self.connection:
			return None

		if nice and self.raw_requestline:
			return None

		if hasattr(self.connection, 'wake'):
			self.connection.wake()

	def get_name(self):
		if self.request is None:
			return '%s.%s' % (
				self.__class__.__module__,
				self.__class__.__name__)
		else:
			return self.request.get_name()

	def traceback(self, level = logging.ERROR):
		super(HttpProtocol, self).traceback(level)

		if level < logging.INFO:
			return None

		if self._tbrec is None:
			return None

		self._tbrec.record(name = self.get_name())

	def sent_headers(self):
		return self._sent_headers


class HttpRequest(object):
	request_count = 0
	# <path>;<params>?<query>#<fragment>
	path_re = re.compile('([^;?#]*)(;[^?#]*)?(\?[^#]*)?(#.*)?')
	cookies = {}

	def __init__(self, connection, requestline, command, path, headers):
		HttpRequest.request_count = HttpRequest.request_count + 1
		self._request_number = HttpRequest.request_count
		self.requestline = requestline
		self._request_headers = headers
		self._connection = connection
		#
		# request is named by handler for stats collection
		#
		self._name = 'none'

		## By the time we get here, BaseHTTPServer has already
		## verified that the request line is correct.
		self._method = command.lower()
		self._uri = path

		m = HttpRequest.path_re.match(self._uri)
		self._path, self._params, self._query, self._frag = m.groups()

		if self._query and self._query[0] == '?':
			self._query = self._query[1:]
		#
		# unquote the path, other portions of the uri are unquoted
		# where they are handled
		#
		self._path = urllib.unquote_plus(self._path)
		self.cookie_domain = None
	#
	# statistics/information related functions.
	# name should be set by request handler and used for statistics gathering
	#
	def set_name(self, o):
		if inspect.isclass(type(o)):
			o = type(o)
		if inspect.isclass(o):
			o = '%s.%s' % (o.__module__, o.__name__)
		if type(o) == type(''):
			self._name = o

	def get_name(self):
		return self._name
	#
	# some pass through functions to the connection
	#
	def log_level(self, level):
		'''log_level

		Set the coroutine log level for this request.
		'''
		self._connection.req_log_level(level)

	def push(self, s, encode = False):
		'''push

		Given a string push the value to the request client. The first
		push for a request will generate and flush headers as well.
		An optional encode parameter, when set to True, will attempt
		a content encoding on the string.

		NOTE: When encode is True the entire body of the response MUST
		      be pushed, since the encode cannot be partial. IF a
		      susequent push is performed on the same request after an
		      encode has occured, an exception will be raised.
		'''		
		return self._connection.push(s, encode = encode)

	def set_header(self, key, value, **kwargs):
		return self._connection.set_header(key, value, **kwargs)

	def get_outgoing_header(self, key, default = None):
		return self._connection.get_outgoing_header(key, default)

	def has_outgoing_header(self, key):
		return self._connection.has_outgoing_header(key)

	def pop_outgoing_header(self, key):
		return self._connection.pop_outgoing_header(key)

	def has_key(self, key):
		return self.has_outgoing_header(key)

	def push_chunked(self, s):
		return self._connection.push_chunked(s)

	def response(self, code = 200):
		return self._connection.response(code)

	def send_error(self, code, message = None):
		return self._connection.send_error(code, message)

	def server(self):
		return self._connection.server

	def proto(self):
		return float(self._connection.request_version.split('/')[1])

	# Method access
	def method(self):
		return self._method

	# URI access
	def uri(self):
		return self._uri

	def address_string(self):
		for name in HEADER_CLIENT_IPS:
			ip = self.get_header(name)
			if ip: return ip

		return str(self._connection.client_address[0])

	# Incoming header access
	def get_header(self, header_name, default=None):
		"""Get a header with the given name. If none is present,
		return default. Default is None unless provided.
		"""
		return self.get_headers().get(header_name.lower(), default)

	def get_headers(self):
		return self._request_headers

	def get_query_pairs(self):
		"""get_query_pairs

		Return a tuple of two-ples, (arg, value), for
		all of the query parameters passed in this request.
		"""
		if hasattr(self, '_split_query'):
			return self._split_query

		self._split_query = []

		if self._query is None:
			return self._split_query

		for value in self._query.split('&'):
			value = value.split('=')
			key   = value[0]
			value = '='.join(value[1:])

			if key and value:
				self._split_query.append(
					(urllib.unquote_plus(key), urllib.unquote_plus(value)))

		return self._split_query

	# Query access
	def get_query(self, name):
		"""Generate all query parameters matching the given name.
		"""
		for key, value in self.get_query_pairs():
			if key == name or not name:
				yield value

	# Post argument access
	def get_arg_list(self, name):
		return self.get_field_storage().getlist(name)

	def get_arg(self, name, default=None):
		return self.get_field_storage().getfirst(name, default)

	def get_field_storage(self):
		if not hasattr(self, '_field_storage'):
			if self.method() == 'get':
				data = ''
				if self._query:
					data = self._query

				fl = cStringIO.StringIO(data)
			else:
				fl = self._connection.rfile
			## Allow our resource to provide the FieldStorage instance for
			## customization purposes.
			headers = self.get_headers()
			environ = dict(
				REQUEST_METHOD = 'POST',
				QUERY_STRING   = self._query or '')

			if (hasattr(self, 'resource') and
				hasattr(self.resource, 'getFieldStorage')):

				self._field_storage = self.resource.getFieldStorage(
					fl, headers, environ)
			else:
				self._field_storage = cgi.FieldStorage(
					fl, headers, environ = environ)

		return self._field_storage

	def get_cookie(self, name = None, default = None, morsel = False):
		'''get_cookie

		Return a Cookie.SimpleCookie() object containing the request
		cookie.

		Optional parameters:

		  name    - Return a specific cookie value.
		  morsel  - If True then the name/value will be wrapped in a
		            Cookie.Morsel() object, (default: False) instead
			        of the actual value string.
		  default - If the name parameter is specified and the specified
		            name is not found in the cookie then the provided
		            default will be returned instead of None.
		'''
		if not hasattr(self, '_simple_cookie'):
			cookie = self.get_header('Cookie', default='')
			self._simple_cookie = Cookie.SimpleCookie()
			self._simple_cookie.load(cookie)

		if name is None:
			return self._simple_cookie

		data = self._simple_cookie.get(name)
		if data is None:
			return default

		if morsel:
			return data
		else:
			return data.value

	def set_cookie(
		self, name, value,
		domain = None, path = '/', expires = FUTURAMA, strict = False):
		'''set_cookie

		Given a name and value, add a set-cookie header to this request
		objects response.

		Optional parameters:

		domain  - Set the cookie domain. If a cookie domain is not provided
		          then the objects cookie_domain member will be used as the
		          domain. If the cookie_domain member has not been set then
		          the requests Host header will be used to determine the
		          domain. Specifically the N-1 segments of the host or the
		          top 2 levels of the domain which ever is GREATER.
		strict  - If set to True then raise an error if neither the domain
		          parameter or cookie_domain member is set. In other words
		          do NOT derive the domain from the Host header.
		          (default: False)
		expires - Set the cookie expiration time. (default: far future)
		          Use empty string expires value for session cookies.
				  (i.e. cookies that expire when the browser is closed.)
		path    - Set the cookie path. (default: /)
		'''
		if domain is None:
			if self.cookie_domain is None:
				if strict:
					raise LookupError('no domain set w/ strict enforcement')

				host = self.get_header('host')
				if host is None:
					raise ValueError('no host header for cookie inheritance')

				host = host.split('.')
				chop = max(len(host) - 1, min(len(host), 2))
				host = host[-chop:]

				if len(host) < 2:
					raise ValueError(
						'bad host header for cookie inheritance',
						self.get_header('host'))

				domain = '.'.join(host)
			else:
				domain = self.cookie_domain

		domain = domain.split(':')[0]		
				
		morsel = Cookie.Morsel()
		morsel.set(name, value, value)

		morsel['domain']  = '.%s' % domain
		morsel['path']    = path
		morsel['expires'] = expires

		self.set_header('Set-Cookie', morsel.OutputString(), overwrite = False)

	def write(self, stuff):
		#
		# this is where the templating stuff is
		# Hook for converting from random objects into html
		#
		if hasattr(self, 'convert'):
			converted = self.convert(self, stuff)
		else:
			converted = stuff

		self.connection().set_header('Content-Length', len(converted))
		#
		# since write is a one shot process, no follow-up push/writes
		# are expected or encouraged, we are safe to attempt an encoding.
		# check to see if headers have been sent, since some error/exotic
		# paths may send ahead of the framework write.
		#
		encode = not self.connection().sent_headers()
		self.push(converted, encode = encode)

	def connection(self):
		return self._connection

	def traceback(self, level = logging.ERROR):
		return self._connection.traceback(level = level)

	def __setitem__(self, key, value):
		self._connection.set_header(key, value)

	request = property(lambda self: self)

class HttpFileHandler(object):
	def __init__(self, doc_root):
		self.doc_root = doc_root

	def match(self, request):
		path = request._path
		filename = os.path.join(self.doc_root, path[1:])
		if os.path.exists(filename):
			return True
		return False

	def handle_request(self, request):
		request.set_name(self)

		path = request._path
		filename = os.path.join(self.doc_root, path[1:])

		if os.path.isdir(filename):
			filename = os.path.join(filename, 'index.html')

		if not os.path.isfile(filename):
			request.send_error(404)
		else:
			f = file(filename, 'rb')
			finfo = os.stat(filename)
			request.set_header('Content-Type', 'text/html')
			request.set_header('Content-Length', str(finfo.st_size))
			bc = 0

			block = f.read(8192)
			if not block:
				request.send_error(204) # no content
			else:
				while 1:
					bc = bc + request.push(block)
					block = f.read(8192)
					if not block:
						break

class HttpStatisticsHandler(object):
	def __init__(self, allow = [], name = 'statistics'):
		self._name  = name
		self._allow = HttpAllow(allow)

	def match(self, request):
		if request.proto() < 1.0:
			return False
		
		if self._name != request._path.strip('/'):
			return False
		else:
			return self._allow.match(request.connection().address_string())

	def handle_request(self, request):
		request.set_name(self)

		server = request.server()
		data = 'total:'

		results = server.request_averages()
		data += ' %d %d %d %d'   % tuple(map(lambda x: x['count'], results))
		data += ' %d %d %d %d\n' % tuple(map(lambda x: x['elapse'], results))
		
		results = server.request_details()
		results = results.items()
		results.sort()

		for name, values in results:
			data += '%s:' % (name,)

			for value in values:
				data += ' %d' % (value['count']/value['seconds'])

			for value in values:
				if value['count']:
					result = value['elapse']/value['count']
				else:
					result = 0

				data += ' %d' % result

			data += '\n'

		request.set_header('Content-Type', 'text/plain')
		request.response(200)
		request.push(data)

class HttpServer(coro.Thread):
	def __init__(self, *args, **kwargs):
		super(HttpServer, self).__init__(*args, **kwargs)

		self._handlers = []
		self._max_requests = 0
		self._outstanding_requests = {}
		self._exit = False
		self._headers = [HOST_HEADER]
		self._encblack = None
		self._graceperiod = 0
		self._send_size = SEND_BUF_SIZE
		self._recv_size = RECV_BUF_SIZE
		self._connects = 0
		self._requests = 0
		self._response = 0
		self._recorder = statistics.Recorder()
		self._stoptime = 30

		self._wall_time = statistics.TopRecorder(threshold = 0.0)
		self._exec_time = statistics.TopRecorder(threshold = 0.0)
		self._nyld_time = statistics.TopRecorder(threshold = 0.0)
		self._resu_time = statistics.TopRecorder(threshold = 0)
		#
		# mark whether socket was provided to ensure creator is always
		# the destructor as well.
		#
		self.socket = kwargs.get('socket', None)
		self.passed = bool(self.socket is not None)
		self._tbrec = kwargs.get('tbrec', None)
		self._debug = False
		#
		# post request callbacks.
		#
		preq = kwargs.get('postreq', [])
		preq = (isinstance(preq, (list, tuple)) and [preq] or [[preq]])[0]

		self._postreqs = preq

	def statistics(self, allow):
		'''statistics

		Enable IP addresses in the 'allow' list to access server
		statistics through a 'GET /statistics' request.
		'''
		self.push_handler(HttpStatisticsHandler(allow))
		
	def push_default_headers(self, data, merge = True):
		if not merge:
			self._headers.extend(data)

		headers = set(map(lambda i: i[0], self._headers))
		for header, value in data:
			if header in headers:
				self._headers = filter(lambda i: i[0] != header, self._headers)

			self._headers.append((header, value))			

	def push_handler(self, handler):
		self._handlers.append(handler)

	def replace_handler(self, old_handler, new_handler):
		"""replace_handler replaces old_handler with new_handler in
		this http servers handlers list.

		Returns old_handler on success, raises ValueError if
		old_handler is not in the handlers list.
		"""
		for i in xrange(len(self._handlers)):
			if self._handlers[i] == old_handler:
				self._handlers[i] = new_handler
				return old_handler

		raise exceptions.ValueError('%s not in handlers' % str(old_handlers))

	def drop_handlers(self):
		self._handlers = []

	def push_encode_blacklist(self, data):
		'''push_encode_blacklist

		Add a response encoding blacklist.

		NOTE: the default is no blacklist, which means that no encoding
		      will be performed. To encode all responses, push an empty
			  blacklist.
		'''
		self._encblack = data

	def socket_init(self, addr):
		'''socket_init

		create listen socket if it does not already exist.
		'''
		if self.socket is not None:
			return None

		self.socket = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.passed = False
		
		self.socket.set_reuse_addr()
		self.socket.bind(addr)
		self.socket.listen(1024)

	def socket_term(self):
		'''socket_term

		close and delete the listen socket if the server created it.
		'''
		if self.passed:
			return None

		if self.socket is None:
			return None

		self.socket.close()
		self.socket = None

	def set_debug_read(self, flag=True):
		"""set_debug_read

		Call to set (or unset) the _debug_read flag.  If this flag is
		set, data received in calls to read and readlines will be
		logged.
		"""
		self._debug = flag
		for child in self.child_list():
			child.set_debug_read(self._debug)

	def run(self, addr = None, logfile = '', timeout = None, idle = None):
		self._daily = []
		self._hourly = []

		self._idletime = idle
		self._timeout  = timeout

		hndlr = logging.handlers.RotatingFileHandler(
			logfile or 'log', 'a', ACCESS_LOG_SIZE_MAX, ACCESS_LOG_COUNT_MAX)
		hndlr.setFormatter(logging.Formatter('%(message)s'))

		self.access = logging.Logger('access')
		self.access.addHandler(hndlr)

		self.socket_init(addr)

		address, port = self.socket.getsockname()
		self.port = port

		self.info('HttpServer listening on %s:%d' % (address, port))
		while not self._exit:
			try:
				sock, address = self.socket.accept()
				sock.settimeout(self._timeout)
				sock.setsockopt(
					socket.SOL_SOCKET, socket.SO_SNDBUF, self._send_size)
				sock.setsockopt(
					socket.SOL_SOCKET, socket.SO_RCVBUF, self._recv_size)

				protocol = HttpProtocol(
					tbrec = self._tbrec,
					postreq = self._postreqs,
					debug_read = self._debug,
					args = (sock, address, self, self._handlers))
				## This is for any sockets that may happen to get opened
				## during the processing of these http requests; additional
				## outgoing requests made by the server in order to get 
				## data to fulfill an incoming web request, etc.
				protocol.set_socket_timeout(self._idletime)
				protocol.set_default_headers(self._headers)
				protocol.set_encode_blacklist(self._encblack)
				protocol.start()
			except coro.CoroutineSocketWake:
				continue
			except coro.TimeoutError, e:
				self.warn('Http Server: socket timeout: %r' % (e,))
				continue
			except socket.error, e:
				self.warn('Http Server: socket error: %s' % str(e))
				continue
			except exceptions.Exception, e:
				self.error('Http Server: exception: %r' % (e,))
				self.traceback()
				break

		self.info('HttpServer exiting (children: %d)' % self.child_count())
		#
		# stop listening for new connections
		#
		self.socket_term()
		#
		# yield to allow any pending protocol threads to startup.
		# (so we can shut them down :)
		#
		self.Yield(timeout = 0)
		#
		# mark all child connections as in close
		#
		for client in self.child_list():
			client.close_connection = 1
		#
		# wait for a grace period before force closing any open
		# connections which are waiting for requests
		#
		if self.child_list():
			self.Yield(self._graceperiod)

		for client in self.child_list():
			client.shutdown(nice = True)
		#
		# wait for all children to exit
		#
		zombies = self.child_wait(self._stoptime)
		if not zombies:
			return None

		self.info('httpd server timeout on %d zombies' % (zombies))
		for zombie in self.child_list():
			self.info(
				'  httpd zombie: %r <%s>' % (
					zombie.client_address,
					zombie.requestline))

		return None

	def complete(self):
		self._handlers = []
		self._max_requests = 0
		self._outstanding_requests = {}

	def record_counts(self, requests):
		self._connects += 1
		if requests:
			self._requests += requests
			self._response += 1

	def request_started(self, request, current = None):
		"""request_started

		Called back from HttpProtocol when an individual request is
		starting. Can be either a single socket with non-persistent
		connections, or a single request of many on a socket with
		persistent connections.
		"""
		#
		# save request for duration
		#
		self._outstanding_requests[request] = request
		#
		# count max outstanding requests
		#
		self._max_requests = max(
			len(self._outstanding_requests), self._max_requests)

	def request_ended(self, req, code, start_time, push_time, rsize, wsize):
		"""request_ended

		Called back from HttpProtocol when an individual request is
		finished, erronious or not. Must be paired with request_started.
		"""

		if self._exit:
			return None
		#
		# get a single fix on the time
		#
		current = time.time()
		#
		# fixup times
		#
		total_time = max(current - start_time, 0)
		local_time = max(push_time - start_time, 0)
		#
		# record local_time by request handler
		#
		self._recorder.request(
			local_time,
			name = req.get_name(),
			current = current)
		#
		# clear outstanding request
		#
		if req in self._outstanding_requests:
			del(self._outstanding_requests[req])
		#
		# save N most expensive requests
		#
		data = (
			req._uri,
			local_time,
			coro.current_thread().total_time(),
			coro.current_thread().resume_count(),
			coro.current_thread().long_time())

		self._wall_time.save(data[1], data)
		self._exec_time.save(data[2], data)
		self._nyld_time.save(data[3], data)
		self._resu_time.save(data[4], data)
		#
		# log file
		#
		self.access.info(
			'0 - - [%s] "%s" %s %s %s %d %d %s %d' % (
			req.connection().log_date_time_string(),
			req.requestline,
			code, total_time, local_time, rsize, wsize,
			req.connection().address_string(),
			coro.current_id()))
		#
		# Call any request completion callbacks
		#
		for call in self._postreqs:
			if not callable(call):
				continue

			try:
				call(
					req,
					code = code,
					start_time = start_time,
					push_time = push_time,
					current_time = current,
					read_size = rsize,
					write_size = wsize)
			except:
				self.error('TB in %r' % call)
				self.traceback()

		return None
		
	def outstanding_requests(self):
		"""outstanding_requests

		Call me to find out which requests are outstanding.
		"""
		return self._outstanding_requests.values()

	def max_requests(self):
		"""max_requests

		Call me to find out what the max concurrent requests high-water
		mark was.
		"""
		return self._max_requests

	def num_requests(self):
		"""num_requests

		Call me to find out how many requests I am currently handling.
		"""
		return len(self._outstanding_requests)

	def request_rate(self):
		return self._recorder.rate()

	def request_details(self):
		return self._recorder.details()

	def request_averages(self):
		return self._recorder.averages()

	def shutdown(self, grace = 0, stop = 30):
		"""shutdown

		Call me to stop serving new requests and shutdown as soon
		as possible.
		"""
		if self._exit:
			return None

		self._graceperiod = grace
		self._stoptime    = stop
		self._exit        = True

		if hasattr(self.socket, 'wake'):
			return self.socket.wake()

	def get_name(self):
		return '%s.%s' % (
			self.__class__.__module__,
			self.__class__.__name__)

	def traceback(self):
		super(HttpServer, self).traceback()

		if self._tbrec is None:
			return None

		self._tbrec.record(name = self.get_name())
#
# standalone test interface
#
def run(port, log, loglevel, access, root, backport):
	#
	# webserver and handler
	server = HttpServer(
		log = log, args=(('0.0.0.0', port),), kwargs={'logfile': access})
	handler = HttpFileHandler(root)
	server.push_handler(handler)
	server.set_log_level(loglevel)
	server.start()
	#
	# backdoor
	bdserv = backdoor.BackDoorServer(kwargs = {'port': backport})
	bdserv.start()
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

COMMAND_LINE_ARGS = [
	'help', 'fork', 'port=', 'accesslog=', 'backdoor=', 'logfile=',
	'loglevel=', 'root=']

def usage(name, error = None):
	if error:
		print 'Error:', error
	print "  usage: %s [options]" % name
 
def main(argv, environ):
	progname = sys.argv[0]

	backport = 9876
	mainport = 7221
	accesslog = None
	logfile = None
	loglevel = 'INFO'
	dofork = False
	forklist = [progname]
	smap = []
	docroot = '/Library/WebServer/Documents'

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
		elif field == '--backdoor':
			backport = int(val)
		elif field == '--port':
			mainport = int(val)
		elif field == '--accesslog':
			accesslog = val
		elif field == '--logfile':
			logfile = val
		elif field == '--loglevel':
			loglevel = val
		elif field == '--root':
			docroot = val
		elif field == '--fork':
			dofork = True
			continue

		forklist.append(field)
		if val:
			forklist.append(val)

	if dofork:
		pid = os.fork()
		if pid:
			return
		else:
			os.execvpe(progname, forklist, environ)
		
	if logfile:
		hndlr = logging.FileHandler(logfile)
		
		os.close(sys.stdin.fileno())
		os.close(sys.stdout.fileno())
		os.close(sys.stderr.fileno())
	else:
		hndlr = logging.StreamHandler(sys.stdout)
		
	log = coro.coroutine_logger('corohttpd')
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

	run(mainport, log, loglevel, accesslog, docroot, backport)
	return None

if __name__ == '__main__':
	main(sys.argv, os.environ)

