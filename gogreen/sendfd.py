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

'''sendfd.py

Wrapper functions for sending/receiving a socket between two
unix socket domain endpoints in a coro environment
'''

import os
import struct
import socket

import coro
import sendmsg


def sendsocket(control, identifier, sock):
	payload = struct.pack('i', sock.fileno())

	control.wait_for_write()
	sendmsg.sendmsg(
		control.fileno(),
		identifier,
		0,
		(socket.SOL_SOCKET, sendmsg.SCM_RIGHTS, payload))


def recvsocket(control):
	control.wait_for_read()
	result = sendmsg.recvmsg(control.fileno())

	identifier, flags, [(level, type, data)] = result

	fd = struct.unpack('i', data)[0]
	try:
		sock = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
		sock = coro.coroutine_socket(sock)
	finally:
		os.close(fd)

	return sock



