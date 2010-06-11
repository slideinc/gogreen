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

'''dqueue

Object and Queue class for implementing a doubly linked list queue.
'''

import exceptions
import operator

class ObjectQueueError(exceptions.Exception):
	pass

class QueueObject(object):
	__slots__ = ['__item_next__', '__item_prev__']

	def __init__(self, *args, **kwargs):
		self.__item_next__ = None
		self.__item_prev__ = None

	def __queued__(self):
		return not (self.__item_next__ is None or self.__item_prev__ is None)

	def __clip__(self):
		'''__clip__

		void queue connectivity, used when object has been cloned/copied
		and the references are no longer meaningful, since the queue
		neighbors correctly reference the original object.
		'''
		self.__item_next__ = None
		self.__item_prev__ = None

class QueueIterator(object):
	def __init__(self, queue, *args, **kwargs):
		self._queue  = queue
		self._object = self._queue.look_head()

	def __iter__(self):
		return self

	def next(self):
		if self._object is None:
			raise StopIteration

		value, self._object = self._object, self._queue.next(self._object)
		if self._object is self._queue.look_head():
			self._object = None

		return value

class ObjectQueue(object):
	def __init__(self, *args, **kwargs):
		self._head = None
		self._size = 0

	def __len__(self):
		return self._size

	def __del__(self):
		return self.clear()

	def __nonzero__(self):
		return self._head is not None

	def __iter__(self):
		return QueueIterator(self)

	def _validate(self, obj):
		if not hasattr(obj, '__item_next__'):
			raise ObjectQueueError, 'not a queueable object'
		if not hasattr(obj, '__item_prev__'):
			raise ObjectQueueError, 'not a queueable object'

	def put(self, obj, fifo = False):
		self._validate(obj)

		if self._in_queue(obj):
			raise ObjectQueueError, 'object already queued'

		if self._head is None:
			obj.__item_next__ = obj
			obj.__item_prev__ = obj
			self._head        = obj
		else:
			obj.__item_next__ = self._head
			obj.__item_prev__ = self._head.__item_prev__

			obj.__item_next__.__item_prev__ = obj
			obj.__item_prev__.__item_next__ = obj

			if fifo:
				self._head = obj

		self._size += 1

	def get(self, fifo = False):
		if self._head is None:
			return None

		if fifo:
			obj = self._head
		else:
			obj = self._head.__item_prev__

		if obj.__item_next__ is obj and obj.__item_prev__ is obj:
			self._head = None
		else:
			obj.__item_next__.__item_prev__ = obj.__item_prev__
			obj.__item_prev__.__item_next__ = obj.__item_next__

			self._head = obj.__item_next__

		obj.__item_next__ = None
		obj.__item_prev__ = None

		self._size -= 1
		return obj

	def look(self, fifo = False):
		if self._head is None:
			return None

		if fifo:
			return self._head
		else:
			return self._head.__item_prev__

	def remove(self, obj):
		self._validate(obj)

		if not self._in_queue(obj):
			raise ObjectQueueError, 'object not queued'

		if obj.__item_next__ is obj and obj.__item_prev__ is obj:
			self._head = None
		else:
			next = obj.__item_next__
			prev = obj.__item_prev__
			next.__item_prev__ = prev
			prev.__item_next__ = next

			if self._head == obj:
				self._head = next

		obj.__item_next__ = None
		obj.__item_prev__ = None

		self._size -= 1

	def next(self, obj):
		if self._head == obj.__item_next__:
			return None
		else:
			return obj.__item_next__

	def prev(self, obj):
		if self._head.__item_prev__ == obj.__item_prev__:
			return None
		else:
			return obj.__item_prev__

	def put_head(self, obj):
		return self.put(obj, fifo = True)

	def put_tail(self, obj):
		return self.put(obj, fifo = False)

	def get_head(self):
		return self.get(fifo = True)

	def get_tail(self):
		return self.get(fifo = False)

	def look_head(self):
		return self.look(fifo = True)

	def look_tail(self):
		return self.look(fifo = False)

	def clear(self):
		while self:
			self.get()

	def _in_queue(self, obj):
		return obj.__item_next__ is not None and obj.__item_prev__ is not None

def iterqueue(queue, forward = True):
	'''emits elements out of the queue in a given order/direction.'''
	if forward:
		start = queue.look_head
		move  = queue.next
	else:
		start = queue.look_tail
		move  = queue.prev
	item = start()
	while item:
		yield item
		item = move(item)

#
# end...
