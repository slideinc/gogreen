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

"""coutil

Various coroutine-safe utilities.

Written by Libor Michalek.
"""

import os
import sys
import string
import types
import random as whrandom

import coro

class object_queue:

  def __init__ (self):
    # object queue
    self._queue = []
    self._c = coro.coroutine_cond()
    
  def __len__ (self):
    return len(self._queue)

  def push (self, q):
    # place data in the queue, and wake up a consumer
    self._queue.append(q)
    self._c.wake_one()
      
  def pop (self):
    # if there is nothing in the queue, wait to be awoken
    while not len(self._queue):
      self._c.wait()

    item = self._queue[0]
    del self._queue[0]
    
    return item

class critical_section:

  def __init__(self):

    self._lock  = 0
    self._error = 0
    self._c     = coro.coroutine_cond()

    return None

  def get_lock(self):

    while self._lock and not self._error:
      self._c.wait()

    if not self._error:
      self._lock = 1

    return self._error

  def release_lock(self):

    self._lock = 0
    self._c.wake_one()

    return None

  def error(self):

    self._lock  = 0
    self._error = 1
    self._c.wake_all()

    return None

class conditional_id:

  def __init__(self):

    self.__wait_map = {}
    self.__map_cond = coro.coroutine_cond()
     
  def wait(self, id):

    self.__wait_map[id] = coro.current_thread().thread_id()
    self.__map_cond.wait()

    return None
  
  def wake(self, id):

    if self.__wait_map.has_key(id):
      self.__map_cond.wake(self.__wait_map[id])
      del self.__wait_map[id]

    return None

  def wake_one(self):

    if len(self.__wait_map):
      id = whrandom.choice(self.__wait_map.keys())
      self.wake(id)

    return None

  def wake_all(self):

    self.__wait_map = {}
    self.__map_cond.wake_all()

    return None
