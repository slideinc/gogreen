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

"""Tests for corohttpd. Starts an actual server instance and
talks http protocol to it after making a connection to the port
the server is listening on.
"""

import socket
import cStringIO
import logging
import tempfile
import os.path

from gogreen import corohttpd
from gogreen import coro


coro.socket_emulate()


lines = [
	"GET / HTTP/1.1",
	"Host: localhost",
	"Some-header: some-value",
	"Connection: close",
	"",
	""
]

POST = [
	"POST / HTTP/1.1",
	"Host: localhost",
	"Connection: close",
	"Content-length: 8",
	"",
	"asdf=foo"
]


KEEP_ALIVE = [
	"GET / HTTP/1.1",
	"Host: localhost",
	"Connection: keep-alive",
	"Content-Length: 0",
	"",
	""
]


OUTPUT = 'hello world'


def connect_socket(child_port):
	sock = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.settimeout(0.25)
	sock.connect(('127.0.0.1', child_port))
	return sock


def make_request(lines, child_port, sep='\r\n', sock=None, sentinel=None):
	if sock is None:
		sock = connect_socket(child_port)

	stuff = '\r\n'.join(lines)
	left = len(stuff)
	while left:
		sent = sock.send(stuff)
		stuff = stuff[:sent]
		left -= sent
	result = ''
	while True:
		read = sock.recv(32*1024)
		if not read:
			break
		result += read
		if sentinel is not None and sentinel in result:
			break
	return result


def corotest(method):
	failure = []
	result = []
	def method_caller(self, *args, **kw):
		try:
			try:
				rv = method(self, *args, **kw)
				if rv:
					result.append(rv)
			except Exception, e:
				print self.get_output()
				coro.current_thread().traceback()
				failure.append(e)
		finally:
			self.server.shutdown()

	def decorator(self, *args, **kw):
		coro.spawn(method_caller, self, *args, **kw)
		coro.event_loop(0.1)
		if failure:
			raise failure[0]
		if result:
			result[0]()

	return decorator


class MegaHandler(object):
	def match(self, request):
		return True

	def handle_request(self, request):
		assert request.get_header("some-header") == "some-value"
		request.push(OUTPUT)


class BaseTest(object):
	handler_factory = MegaHandler
	def setUp(self):
		log = coro.coroutine_logger('tests')
		self.output = cStringIO.StringIO()
		log.addHandler(logging.StreamHandler(self.output))
		self.server = corohttpd.HttpServer(
			args=(('0.0.0.0', 0), tempfile.mktemp()), log=log)
		self.server.push_handler(self.handler_factory())
		self.server.start()

	def get_output(self):
		self.output.seek(0)
		return self.output.read()


class TestCoroHttpd(BaseTest):
	@corotest
	def test_normal_request(self):
		result = make_request(lines, self.server.port)
		assert result.endswith(OUTPUT)

	@corotest
	def test_unix_newline(self):
		result = make_request(lines, self.server.port, '\n')
		assert result.endswith(OUTPUT)

	@corotest
	def test_close_connection(self):
		sock = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect(('127.0.0.1', self.server.port))
		sock.send(' ')
		sock.close()

		def when_done():
			output = self.get_output()
			assert 'Remote host closed connection before sending request' in output
		return when_done

	@corotest
	def test_bad_request(self):
		bad_request = [
			"asdfkhaskdfhewfiaeifahisefhaiehfiashefiashdf",
			"asdfk asdf asi dfhasi dfhasdfh ishad fh",
			"asdfinasdn asdfni asnfians df ",
			"dsfijasdf asdfasdf ",
			"",
			"",
			""]
		result = make_request(
			bad_request, self.server.port)
		assert '400' in result


class BadException(Exception):
	def __repr__(self):
		return "BadException(%r)" % (self.args[0], )


class BadHandler(object):
	def match(self, request):
		return True

	def handle_request(self, request):
		raise BadException("ono!")


class TestBadHandler(BaseTest):
	handler_factory = BadHandler

	@corotest
	def test_it(self):
		result = make_request(lines, self.server.port)
		assert result.startswith('HTTP/1.1 500')
		assert 'ono!' in self.get_output()


class NoMatchHandler(object):
	def match(self, request):
		return False


class TestNoMatch(BaseTest):
	handler_factory = NoMatchHandler

	@corotest
	def test_it(self):
		result = make_request(lines, self.server.port)
		assert result.startswith('HTTP/1.1 404')


class PostHandler(object):
	def match(self, request):
		return True

	def handle_request(self, request):
		content_length = int(request.get_header('content-length'))
		read = request.connection().read(content_length)
		assert request.method() == 'post'
		request.push(read)


class TestRequestRead(BaseTest):
	handler_factory = PostHandler

	@corotest
	def test_it(self):
		result = make_request(POST, self.server.port)
		assert result.startswith('HTTP/1.1 200')
		assert result.endswith('asdf=foo')

	@corotest
	def test_incomplete(self):
		POST_COPY = POST[:]
		# Send an incomplete body
		POST_COPY[-1] = 'as'
		sock = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect(('127.0.0.1', self.server.port))
		sock.send('\r\n'.join(POST_COPY))
		sock.close()

		def when_done():
			output = self.get_output()
			assert 'Remote host closed connection in read' in  output
		return when_done


class FieldStorageHandler(object):
	def match(self, request):
		return True

	def handle_request(self, request):
		fs = request.get_field_storage()
		asdf = fs.getfirst('asdf')
		assert asdf == 'foo'
		request.push(asdf)


class TestFieldStorage(BaseTest):
	handler_factory = FieldStorageHandler

	@corotest
	def test_it(self):
		result = make_request(POST, self.server.port)
		assert result.startswith('HTTP/1.1 200')
		assert result.endswith('foo')


def file_handler(self):
	dir = tempfile.mkdtemp()
	open(os.path.join(dir, 'index.html'), 'w').write('goodbye world')
	os.mkdir(os.path.join(dir, 'foo'))
	open(os.path.join(dir, 'bar.txt'), 'w').close()
	return corohttpd.HttpFileHandler(dir)


class TestFileHandler(BaseTest):
	handler_factory = file_handler

	@corotest
	def test_it(self):
		result = make_request(lines, self.server.port)
		assert result.endswith('goodbye world')

	@corotest
	def test_not_found(self):
		my_lines = [
			"GET /asdfaadsf HTTP/1.1",
			"Host: localhost",
			"",
			""]
		result = make_request(my_lines, self.server.port)
		assert result.startswith("HTTP/1.1 404")

	@corotest
	def test_no_index(self):
		my_lines = [
			"GET /foo HTTP/1.1",
			"Host: localhost",
			"",
			""]
		result = make_request(my_lines, self.server.port)
		assert result.startswith("HTTP/1.1 404")

	@corotest
	def test_no_content(self):
		my_lines = [
			"GET /bar.txt HTTP/1.1",
			"Host: localhost",
			"",
			""]
		result = make_request(my_lines, self.server.port)
		assert result.startswith("HTTP/1.1 204")


class AnyHandler(object):
	def match(self, request):
		return True

	def handle_request(self, request):
		request.push(OUTPUT)


class TestKeepalives(BaseTest):
	handler_factory = AnyHandler

	@corotest
	def test_keepalive(self):
		sock = connect_socket(self.server.port)
		# If we don't get a timeout here we pass
		make_request(
			KEEP_ALIVE, self.server.port, sock=sock, sentinel=OUTPUT)
		make_request(
			KEEP_ALIVE, self.server.port, sock=sock, sentinel=OUTPUT)

		sock.close()
