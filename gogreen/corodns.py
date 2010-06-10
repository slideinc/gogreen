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

"async resolver with synchronous interface"

DNSLIB_ERROR_MSG = \
"""corodns requires the files dnslib.py, dnsclass.py, dnstype.py, and
dnsopcode.py from the Demo/dns/ subdirectory of the Python distribution.
Copy these files where Python will find them.
"""

import sys

try:
	import dnslib
	import dnsclass
	import dnsopcode
	import dnstype
except ImportError:
	raise SystemExit(DNSLIB_ERROR_MSG)

import fifo
import coro
import socket

# XXX: need to support timeouts.

class resolver:

	def __init__ (self, server='127.0.0.1'):
		self.server = server
		self.socket = coro.coroutine_socket()
		self.socket.create_socket (socket.AF_INET, socket.SOCK_DGRAM)
		# these can be set really high on linux, but freebsd 2 default limit is 256KB
		self.socket.socket.setsockopt (socket.SOL_SOCKET, socket.SO_SNDBUF, 200 * 1024)
		self.socket.socket.setsockopt (socket.SOL_SOCKET, socket.SO_RCVBUF, 200 * 1024)
		# This setting is a function of the receive buffer size.  Assume 1KB per request,
		# and leave plenty of room.
		self.max_outstanding = 100
		self.fifo = fifo.fifo()
		self.request_map = {}
		self.id = 0L

	def build_request (self, name, qtype, qclass, recursion):
		m = dnslib.Mpacker()
		id = self.id % 65536
		self.id = self.id + 1
		m.addHeader (
			id,
			0, dnsopcode.QUERY, 0, 0, recursion, 0, 0, 0,
			1, 0, 0, 0
			)
		m.addQuestion (name, qtype, qclass)
		return m.getbuf(), id

	def send_request (self, (qname, qtype, qclass, recursion), k):
		r, id = self.build_request (qname, qtype, qclass, recursion)
		n = self.socket.sendto (r, (self.server, 53))
		if n != len(r):
			raise socket.error, "sendto() underperformed"
		self.request_map[id] = k

	def enqueue (self, q, k):
		self.fifo.push ((q, k))
		self.maybe_dequeue()

	def maybe_dequeue (self):
		while (len(self.fifo) and (len(self.request_map) < self.max_outstanding)):
			q, k = self.fifo.pop()			
			self.send_request (q, k)

	def handle_reply (self, reply):
		id = (ord(reply[0])<<8) | ord(reply[1])
		if self.request_map.has_key (id):
			k = self.request_map[id]
			del self.request_map[id]
			coro.schedule (k, reply)
		else:
			sys.stderr.write ('*** orphaned DNS reply\n')

	def run (self):
		while 1:
			reply, whence = self.socket.recvfrom (2048)
			self.handle_reply (reply)

cache = {}

def initialize (server='127.0.0.1'):
	global the_resolver
	# TODO: verify that the resolver is there by connecting to
	# it via tcp.
	the_resolver = resolver (server)
	coro.spawn (the_resolver.run)

def query (name, qtype='MX', qclass='IN', recursion=1):
	global the_resolver, cache
	key = (name, qtype, qclass)
	if cache.has_key (key):
		print 'cache hit'
		return cache[key]
	else:
		if the_resolver is None:
			initialize()
		qtype = getattr (dnstype, qtype)
		qclass = getattr (dnsclass, qclass)
		the_resolver.enqueue (
			(name, qtype, qclass, recursion),
			coro.current()
			)
		result = unpack_reply (coro.Yield())
		cache[key] = result
		return result

import exceptions

class DNS_Exception (exceptions.Exception):
	pass

def query_with_cname (name, qtype='IN', qclass='IN', recursion=1, depth=0):
	if depth > 5:
		raise DNS_Exception, 'CNAME loop'
	else:
		result = query (name, qtype, qclass, recursion)
		if qtype=='CNAME':
			return result
		else:
			for r in result.an:
				if r[0] == 'CNAME':
					return query_with_cname (r[2], qtype, qclass, recursion, depth+1)
				else:
					return r[2]
			return None

def gethostbyname (host):
	return query_with_cname (host, 'A')

class dns_reply:
	def __init__ (self):
		self.q  = []
		self.an = []
		self.ns = []
		self.ar = []

	def __repr__ (self):
		return '<dns_reply q:%s an:%s ns:%s ar:%s>' % (self.q, self.an, self.ns, self.ar)

def get_rr (u):
	name, type, klass, ttl, rdlength = u.getRRheader()
	typename = dnstype.typestr(type)
	mname = 'get%sdata' % typename
	if hasattr (u, mname):
		return (typename, name, getattr(u, mname)())
	else:
		return (typename, name, u.getbytes(rdlength))

def unpack_reply (reply):
	u = dnslib.Munpacker (reply)
	(id, qr, opcode, aa, tc, rd, ra, z, rcode,
	 qdcount, ancount, nscount, arcount) = u.getHeader()
	r = dns_reply()
	for i in range(qdcount):
		r.q.append(u.getQuestion())
	for i in range(ancount):
		r.an.append (get_rr(u))
	for i in range(nscount):
		r.ns.append (get_rr(u))
	for i in range(arcount):
		r.ar.append (get_rr(u))
	return r

# To test:
# start this server in one window.
# In another window, telnet to port 8023, and type
# something like this:
#
# >>> query ('yoyodyne.com')
# <dns_reply q:[('yoyodyne.com', 15, 1)] an:[('MX', 'Yoyodyne.COM', (0, 'Try-Again.Adelman.COM')), ('MX', 'Yoyodyne.COM', (10, 'EQL.Caltech.Edu'))] ns:[('NS', 'Yoyodyne.COM', 'NS1.Adelman.COM'), ('NS', 'Yoyodyne.COM', 'EQL.Caltech.Edu'), ('NS', 'Yoyodyne.COM', 'THOR.INNOSOFT.COM')] ar:[('A', 'Try-Again.Adelman.COM', '198.137.202.66'), ('A', 'EQL.Caltech.Edu', '131.215.29.1'), ('A', 'NS1.Adelman.COM', '198.137.202.66'), ('A', 'THOR.INNOSOFT.COM', '192.160.253.66')]>
# >>> 

if __name__ == '__main__':

	import backdoor
	global the_resolver

	if len(sys.argv) > 1:
		initialize (sys.argv[1])
	else:
		initialize ('127.0.0.1')

	def q2 (*args):
		reply = apply (query, args)
		dnslib.dumpM (dnslib.Munpacker (reply))

	coro.spawn (backdoor.serve)
	coro.spawn (the_resolver.run)
	coro.event_loop (30.0)
