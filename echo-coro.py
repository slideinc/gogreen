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

import socket
import sys
import os
import coro
import signal

ECHO_PORT = 5580

class EchoClient(coro.Thread):
	def __init__(self, *args, **kwargs):
		super(EchoClient, self).__init__(*args, **kwargs)

		self.sock = kwargs['sock']
		self.addr = kwargs['addr']
		self.exit = False
		
	def run(self):
		self.info('Accepted connection: %r', (self.addr,))

		while not self.exit:
			try:
				buf = self.sock.recv(1024)
			except coro.CoroutineSocketWake:
				continue
			except socket.error:
				buf = ''

			if not buf:
				break

			try:
				self.sock.send(buf)
			except coro.CoroutineSocketWake:
				continue
			except socket.error:
				break

		self.sock.close()
		self.info('Connection closed: %r', (self.addr,))

	def shutdown(self):
		self.exit = True
		self.sock.wake()

class EchoServer(coro.Thread):
	def __init__(self, *args, **kwargs):
		super(EchoServer, self).__init__(*args, **kwargs)

		self.addr = kwargs['addr']
		self.sock = None
		self.exit = False

	def run(self):
		self.sock = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind(self.addr)
		self.sock.listen(128)

		self.info('Listening to address: %r' % (self.addr,))
	
		while not self.exit:
			try:
				conn, addr = self.sock.accept()
			except coro.CoroutineSocketWake:
				continue
			except Exception, e:
				self.error('Exception from accept: %r', e)
				break

			eclnt = EchoClient(addr = addr, sock = conn)
			eclnt.start()

		self.sock.close()
		
		for child in self.child_list():
			child.shutdown()

		self.child_wait(30)
		self.info('Server exit.')

	def shutdown(self):
		self.exit = True
		self.sock.wake()
		
def main(argv, stdout, environ):
	eserv = EchoServer(addr = ('', ECHO_PORT))
	eserv.start()

	def shutdown_handler(signum, frame):
		eserv.shutdown()

	signal.signal(signal.SIGUSR2, shutdown_handler)

	try:
		coro.event_loop()
	except KeyboardInterrupt:
		pass

	return None

if __name__ == '__main__':
  main(sys.argv, sys.stdout, os.environ)


