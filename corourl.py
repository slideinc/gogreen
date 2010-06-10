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

import os
import sys
import string
import exceptions
import coro
import urlparse
import socket
#
# functions for fetching a url, that can be used in a coroutine
# 
HTTP_VERSION = 'HTTP/1.0'
HTTP_PORT = 80
CLIENT_VERSION = '1.0'


# sentinel value
USE_DEFAULT_TIMEOUT = -1

def geturl(url, timeout=None, connect_timeout=None):

  (scheme, host, path, param, query, frag) = urlparse.urlparse (url)
  
  if scheme != 'http':
    raise NameError, 'Invalid url <%s>, only http supported' % (scheme)

  request = urlparse.urlunparse(('', '', path, param, query, frag))

  h = HTTP(host, timeout=timeout, connect_timeout=connect_timeout)
  h.putrequest ('GET', request)
  h.putheader ('Host', host)
  h.putheader ('User-Agent', 'coroutine_http/' + CLIENT_VERSION)
  h.endheaders ()

  errcode, errmsg, headers, body = h.getreply()
  
  if errcode == 200:
    return body
  else:
    return None

#
# Each instance of the HTTP class is a single 1.0 connection
#
class HTTP_Error (exceptions.Exception):
  pass

class HTTP:

  def __init__(self, host = None, port = None, timeout=None,
	       connect_timeout=None):

    self._send_buffer = ''
    self._recv_buffer = ''
    self._socket = None
    self._timeout = timeout
    self._connect_timeout = connect_timeout
    
    if host is not None:
      self.connect(host, port)
      
    return None

  def _send(self):

    if self._socket is None:
      raise HTTP_Error, "uninitialized connection."

    while len(self._send_buffer):
      
      try:
	send_size = self._socket.send(self._send_buffer)
      except socket.error, why:
        raise HTTP_Error, 'socket error while sending: ' + str(why[0])
      except coro.TimeoutError, why:
        raise HTTP_Error, 'timeout error while sending: ' + str(why[0])

      if send_size:
	self._send_buffer = self._send_buffer[send_size:]
      else:
	raise HTTP_Error, 'socket error while sending'

    return None

  def _recv(self):
    #
    # read data until receiving an 
    #
    recv_data = []
    
    while 1:

      try:
	data_str = self._socket.recv(8192)
      except socket.error, why:
        raise HTTP_Error, 'socket error while recving: ' + str(why[0])
      except coro.TimeoutError, why:
        raise HTTP_Error, 'timeout error while recving: ' + str(why[0])
	
      if len(data_str):
	recv_data.append(data_str)
      else:
        break
      
    self._recv_buffer = string.join(recv_data, '')

    return None

  def send(self, buffer_str):
    self._send_buffer = self._send_buffer + buffer_str

    return None
  
  def connect(self, host, port=None, timeout=USE_DEFAULT_TIMEOUT,
	      connect_timeout=USE_DEFAULT_TIMEOUT):

    if port is None:
      #
      # now port specified, determine if it is part of the host name.
      #
      colon = string.find(host, ':')
      if colon > -1:

        port = host[colon+1:]
        host = host[:colon]

        try:
          port = string.atoi(port)
        except:
          raise coro.CoroutineSocketError, "nonnumeric port"
      else:
        port = HTTP_PORT

    # use init values if no timeouts have been passed in
    if timeout == USE_DEFAULT_TIMEOUT:
      timeout = self._timeout
    if connect_timeout == USE_DEFAULT_TIMEOUT:
      connect_timeout = self._connect_timeout

    self._socket = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM,
				    timeout=timeout,
				    connect_timeout=connect_timeout)
    try:
      self._socket.connect((host, port))
    except coro.TimeoutError, why:
      raise HTTP_Error, 'timeout error while connecting: ' + str(why[0])

  def close(self):

    if self._socket is not None:
      self._socket.close()

    self._socket = None
    self._send_buffer = ''
    self._recv_buffer = ''

    return None
  
  def putrequest(self, request, selector = '/'):

    if len(self._send_buffer):
      raise HTTP_Error, "http request must come before http headers"
    
    str = "%s %s %s\r\n" % (request, selector, HTTP_VERSION)
    self.send(str)

    return None

  def putheader(self, header, content):

    str = "%s: %s\r\n" % (header, content)
    self.send(str)

    return None

  def endheaders(self):

    str = "\r\n"
    self._send_buffer = self._send_buffer + str

    return None

  def _error(self, code = -1, msg = '', header = [], body = ''):
    return code, msg, header, body
  
  def getreply(self):

    try:
      self._send()
      self._recv()
    except HTTP_Error, error:
      return self._error(msg = error)
    #
    # break out the rest of the message
    #
    try:
      [ver, code, rest] = string.split(self._recv_buffer, None, 2)
    except ValueError:
      return self._error()

    if ver[:5] != 'HTTP/':
      return self._error()

    errcode = string.atoi(code)
    
    try:
      [msg, rest] = string.split(rest, "\n", 1)
    except ValueError:
      return self._error(code = errcode)
    else:
      errmsg = string.strip(msg)

    # We have to be careful here, some servers don't properly 
    # end lines with \r\n
    lines = string.split (rest, '\n')
    header = []
    for x in range (len(lines)):
      line = string.strip (lines[x])
      if not line:
        body = string.join (lines[x+1:], '\n')
        break
      else:
        header.append (line)

    return errcode, errmsg, header, body

if __name__ == '__main__':

  def fetch (url):
    filename = url[string.rfind(url, '/')+1:]
    print 'starting %s...' % filename
    open (filename, 'wb').write (geturl (url))
    print 'finished with %s' % filename

  import sys
  urls = sys.argv[1:]
  for url in sys.argv[1:]:
    coro.spawn (fetch, url)
  coro.event_loop (30.0)
