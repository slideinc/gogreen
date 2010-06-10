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

"""
A coroutine-based web crawler that uses corodns, which requires a nameserver
to be running at %(NAMESERVER)s. Usage:

      python crawler.py <URL>
"""
#
# TODO: much better robots.txt comprehension. Right now, the mere *existence*
# of a robots.txt file will deter this timid crawler. This was the result of
# a deliberate decision to write a conservative robot.
#

crawler_debug = 0

USER_AGENT_STRING = 'Python coro-crawler http://www.egroups.com/group/python-coro/'

NAMESERVER = '127.0.0.1'

VERSION_STRING = '$Id: //depot/main/findmail/src/coroutine/crawler.py#4 $'

import string
import coro
import corodns
import coutil
import formatter
import htmllib
import urllib
import urlparse
import re
import socket

binary_files = re.compile (r'.*\.(gif|jpg|jpeg|gz|tar|pdf|ps)', re.IGNORECASE)

def request_text(uri):
	lines = ('GET %s HTTP/1.0' %  uri,
		 	'User-Agent: %s' % USER_AGENT_STRING,
			'Connection: close', '', '')
	return string.join(lines, '\r\n')

def get_tld (host):
	return string.join (string.split (host, '.')[-2:], '.')

def make_tld_filter (tld):

	def tld_filter (host, port, uri, tld=tld):
		m = binary_files.match (uri)
		if m:
			return 0
		else:
			return tld == string.lower (get_tld (host))

	return tld_filter

def url_split (url):
	urltype, url = urllib.splittype (url)
	host, uri = urllib.splithost(url)
	host, port = urllib.splitport (host)
	uri, query = urllib.splitquery (uri)
	uri, tag = urllib.splittag (uri)
	if port is None:
		port = 80
	return urltype, host, port, uri, query, tag

cache = {}

def gethostbyname (host):
	host = string.lower (host)
	if not cache.has_key (host):
		cache[host] = corodns.gethostbyname (host)
	return cache[host]

def get_content_type (header):
	lines = string.split (header, '\r\n')
	for line in lines:
		i = string.find (line, ': ')
		if i != -1:
			name, value = line[:i], line[i+2:]
			if string.lower (name) == 'content-type':
				return value
	return None

cache_robots_ok = {}
def robots_ok(host, port):
	addr = host, port
	if not cache_robots_ok.has_key(addr):
		robots_ok = no_robots_file(host, port)
		cache_robots_ok[addr] = robots_ok
		if crawler_debug:
			if (robots_ok):
				print "No robots.txt file ... continuing."
			else:
				print "A robots.txt file ... stopping."
	return cache_robots_ok[addr]

def no_robots_file(host, port):
	"""Return true if we can positively verify that 'robots.txt'
	   does not exist."""
	s = coro.make_socket (socket.AF_INET, socket.SOCK_STREAM)
	uri = 'http://%s:%d/robots.txt' % (host, port)
	if crawler_debug:
		print '  grabbing %s....' % uri
	# lookup IP address
	if crawler_debug:
		print '  looking up ip..'
	ip = gethostbyname (host)
	if crawler_debug:
		print '  got it', ip
	if not ip:
		return 0
	s.connect ((ip, int(port)))
	s.send (request_text(uri))

	header = ''
	found_header = 0
	while 1:
		block = s.recv (8192)
		if not block:
			break
		else:
			header = header + block
			i = string.find (header, '\r\n\r\n')
			if i != -1:
				found_header = 1
				header = header[:i]
				break
	
	if not found_header:
		return 0
	firstline = string.split(header, '\r\n')[0]

	# A cheap way to detect any 200-level, 'successful' response:
	i = string.find(firstline, '20')
	if i != -1:
		return 0
	# well, then, it don't exist:
	return 1
	

pending = coutil.object_queue()
fetches = {}
fetch_count = 0
working = {}

NextURL = "NextURL"

def fetcher (n):
	global fetch_count, working, fetches, pending
	i = 0
	while 1:
		if (len(pending) == 0) and (len(working)==0):
			print 'done. hit ctrl-c, fred'
		(host, port, uri, url_filter) = pending.pop()
		work_key = (host, port, uri)
		working[work_key] = None
		fetch_count = fetch_count + 1
		i = i + 1
		try:
			# what about alternative ports?
			url = urlparse.urljoin ('http://%s' % host, uri)
			print '%4d:%4d:%4d:%4d %s %s' % (fetch_count, n, i, len(pending), host, uri)
			if not robots_ok(host, port):
				raise NextURL

			fetches[(host, port, uri)] = 1
			f = formatter.NullFormatter()
			p = htmllib.HTMLParser(f)
			s = coro.make_socket (socket.AF_INET, socket.SOCK_STREAM)
			# lookup IP address
			if crawler_debug:
				print 'looking up ip..'
			ip = gethostbyname (host)
			if crawler_debug:
				print 'got it', ip
			if not ip:
				print 'No IP for %s' % host
				return
			s.connect ((ip, int(port)))
			s.send (request_text(uri))

			header = ''
			found_header = 0
			while 1:
				block = s.recv (8192)
				if not block:
					break
				elif found_header:
					p.feed (block)
				else:
					header = header + block
					i = string.find (header, '\r\n\r\n')
					if i != -1:
						found_header = 1
						h = header[:i]
						content_type = get_content_type (header)
						if string.lower (content_type) == 'text/html':
							p.feed (header[i+4:])
						else:
							raise NextURL

			if crawler_debug:
				print "p.anchorlist has length %d" % len(p.anchorlist)
				#print "p.anchorlist is %s" % p.anchorlist
			for a in p.anchorlist:
				urltype, rest = urllib.splittype (a)
				if urltype is None:
					next_url = urlparse.urljoin (url, rest)
				elif urltype == 'http':
					next_url = a
				else:
					next_url = None
				if next_url:
					#print 'next_url', url, next_url
					(urltype, host, port, uri, query, tag) = url_split (next_url)
					if urltype == 'http':
						if not fetches.has_key ((host,port,uri)):
							if url_filter:
								if url_filter (host, port, uri):
									fetches[(host,port,uri)] = 1
									pending.push ((host, port, uri, url_filter))
								else:
									#print 'url_filter eliminated %s' % next_url
									pass
							else:
								fetches[(host,port,uri)] = 1
								pending.push ((host, port, uri, url_filter))
						else:
							#print 'already fetch{ed,ing} %s' % next_url
							pass
			p.close()
			s.close()
		except NextURL:
			pass
		except:
			import traceback
			traceback.print_exc()
		del working[work_key]

def test (url):
	import backdoor
	corodns.initialize(NAMESERVER)
	(urltype, host, port, uri, query, tag) = url_split (url)
	if urltype != 'http':
		raise ValueError, "http only, please"
	pending.push ((host, port, uri, make_tld_filter (get_tld (host))))
	for i in range(20):
		coro.spawn (fetcher, i)
	coro.spawn (backdoor.serve)
	coro.event_loop (30.0)

if __name__ == '__main__':
	import sys
	if len(sys.argv) < 2:
		print __doc__ % vars()
	else:
		test (sys.argv[1])
