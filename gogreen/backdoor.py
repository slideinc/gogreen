#!/usr/bin/env python
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

import coro
import socket
import string
import StringIO
import sys
import traceback

# Originally, this object implemented the file-output api, and set
# sys.stdout and sys.stderr to 'self'.  However, if any other
# coroutine ran, it would see the captured definition of sys.stdout,
# and would send its output here, instead of the expected place.  Now
# the code captures all output using StringIO.  A little less
# flexible, a little less efficient, but much less surprising!
# [Note: this is exactly the same problem addressed by Scheme
#  dynamic-wind facility]

class BackDoorClient(coro.Thread):
	def init(self):
		self.address = None
		self.socket  = None
		self.buffer  = ''
		self.lines   = []
		self.exit    = False
		self.multilines = []
		self.line_separator = '\r\n'
		#
		# allow the user to change the prompts:
		#
		if not sys.__dict__.has_key('ps1'):
			sys.ps1 = '>>> '
		if not sys.__dict__.has_key('ps2'):
			sys.ps2 = '... '

	def send (self, data):
		olb = lb = len(data)
		while lb:
			ns = self.socket.send (data)
			lb = lb - ns
		return olb
		
	def prompt (self):
		if self.multilines:
			self.send (sys.ps2)
		else:
			self.send (sys.ps1)

	def read_line (self):
		if self.lines:
			l = self.lines[0]
			self.lines = self.lines[1:]
			return l
		else:
			while not self.lines:
				block = self.socket.recv (8192)
				if not block:
					return None
				elif block == '\004':
					self.socket.close()
					return None
				else:
					self.buffer = self.buffer + block
					lines = string.split (self.buffer, self.line_separator)
					for l in lines[:-1]:
						self.lines.append (l)
					self.buffer = lines[-1]
			return self.read_line()
		
	def run(self, conn, addr):
		self.socket  = conn
		self.address = addr

		# a call to socket.setdefaulttimeout will mean that this backdoor
		# has a timeout associated with it. to counteract this set the
		# socket timeout to None here.
		self.socket.settimeout(None)
		
		self.info('Incoming backdoor connection from %r' % (self.address,))
		#
		# print header for user
		#
		self.send ('Python ' + sys.version + self.line_separator)
		self.send (sys.copyright + self.line_separator)
		#
		# this does the equivalent of 'from __main__ import *'
		#
		env = sys.modules['__main__'].__dict__.copy()
		#
		# wait for imput and process
		#
		while not self.exit:
			self.prompt()
			try:
				line = self.read_line()
			except coro.CoroutineSocketWake:
				continue
			
			if line is None:
				break
			elif self.multilines:
				self.multilines.append(line)
				if line == '':
					code = string.join(self.multilines, '\n')
					self.parse(code, env)
                    # we do this after the parsing so parse() knows not to do
					# a second round of multiline input if it really is an
					# unexpected EOF
					self.multilines = []
			else:
				self.parse(line, env)

		self.info('Backdoor connection closing')

		self.socket.close()
		self.socket = None
		return None

	def parse(self, line, env):
		save = sys.stdout, sys.stderr
		output = StringIO.StringIO()
		try:
			try:
				sys.stdout = sys.stderr = output
				co = compile (line, repr(self), 'eval')
				result = eval (co, env)
				if result is not None:
					print repr(result)
					env['_'] = result
			except SyntaxError:
				try:
					co = compile (line, repr(self), 'exec')
					exec co in env
				except SyntaxError, msg:
					# this is a hack, but it is a righteous hack:
					if not self.multilines and str(msg) == 'unexpected EOF while parsing':
						self.multilines.append(line)
					else:
						traceback.print_exc()
				except:
					traceback.print_exc()
			except:
				traceback.print_exc()
		finally:
			sys.stdout, sys.stderr = save
			self.send (output.getvalue())
			del output

	def shutdown(self):
		if not self.exit:
			self.exit = True
			self.socket.wake()


class BackDoorServer(coro.Thread):
	def init(self):
		self._exit = False
		self._s    = None

	def run(self, port=8023, ip=''):
		self._s = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
		self._s.set_reuse_addr()
		self._s.bind((ip, port))
		self._s.listen(1024)

		port = self._s.getsockname()[1]
		self.info('Backdoor listening on port %d' % (port,))
		
		while not self._exit:
			try:
				conn, addr = self._s.accept()
			except coro.CoroutineSocketWake:
				continue

			client = BackDoorClient(args = (conn, addr))
			client.start()

		self.info('Backdoor exiting (children: %d)' % self.child_count())

		self._s.close()
		self._s = None

		for child in self.child_list():
			child.shutdown()

		self.child_wait()
		return None

	def shutdown(self):
		if self._exit:
			return None

		self._exit = True

		if self._s is not None:
			self._s.wake()
#
# extremely minimal test server
#
if __name__ == '__main__':
	server = BackDoorServer()
	server.start()
	coro.event_loop (30.0)
#
# end...
