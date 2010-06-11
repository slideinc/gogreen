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

'''cache

various simple caches
'''

from util import dqueue

DEFAULT_CACHE_SIZE = 1024

class CacheObject(dqueue.QueueObject):
	__slots__ = ['id', 'value']

	def __init__(self, *args, **kwargs):
		super(CacheObject, self).__init__(*args, **kwargs)

		self.id    = args[0]
		self.value = kwargs.get('value', None)

	def __repr__(self):
		return '<CacheObject %r: %r>' % (self.id, self.value)


class LRU(object):
	def __init__(self, *args, **kwargs):
		self._size = kwargs.get('size', DEFAULT_CACHE_SIZE)
		self._ordr = dqueue.ObjectQueue()
		self._objs = {}

	def __len__(self):
		return len(self._objs)

	def _balance(self):
		if len(self._objs) > self._size:
			obj = self._ordr.get_tail()
			del(self._objs[obj.id])

	def _lookup(self, id):
		obj = self._objs.get(id, None)
		if obj is not None:
			self._ordr.remove(obj)
			self._ordr.put_head(obj)

		return obj

	def _insert(self, obj):
		self._objs[obj.id] = obj
		self._ordr.put_head(obj)

		self._balance()

	def lookup(self, id):
		obj = self._lookup(id)
		if obj is None:
			return None
		else:
			return obj.value

	def insert(self, id, value):
		obj = self._lookup(id)
		if obj is None:
			obj = CacheObject(id)
			self._insert(obj)

		obj.value = value

	def reset(self, size = None):
		if size is not None:
			self._size = size

		self._ordr.clear()
		self._objs = {}

	def head(self):
		return self._ordr.look_head()

	def tail(self):
		return self._ordr.look_tail()
#
# end...

