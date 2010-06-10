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

"""coroqueue

A corosafe Queue implementation.

Written by Libor Michalek.
"""

import weakref
import coro
import time
import bisect
import exceptions
import operator
import copy
import smtplib
import socket

import pyinfo

DEFAULT_PRIORITY = 0x01


def merge(dst, src):
	if dst is None:
		return copy.deepcopy(src)

	if not isinstance(dst, type(src)):
		raise TypeError(
			'Cannot merge two different types %r:%r' % (type(dst), type(src)))

	if isinstance(dst, type(0)):
		return dst + src

	if isinstance(dst, type(())):
		return tuple(map(operator.add, dst, src))

	if isinstance(dst, type({})):
		return filter(
			lambda i: dst.update({i[0]: merge(dst.get(i[0]), i[1])}),
			src.items()) or dst

	raise TypeError('Unhandled destination type: %r' % (type(dst),))


class Timeout(object):
	def __init__(self, timeout):
		if timeout is None:
			self.expire = None
		else:
			self.expire = time.time() + timeout

	def __repr__(self):
		if self.expire is None:
			return repr(None)
		else:
			return repr(self.expire - time.time())

	def __nonzero__(self):
		if self.expire is None:
			return True
		else:
			return bool(self.expire > time.time())

	def __call__(self):
		if self.expire is None:
			return None
		else:
			return self.expire - time.time()


class ElementContainer(object):
	def __init__(self, *args, **kwargs):
		self._item_list = []
		self._item_set  = set()

	def __len__(self):
		return len(self._item_list)

	def __nonzero__(self):
		return bool(self._item_list)

	def __contains__(self, obj):
		return obj in self._item_set

	def add(self, obj):
		self._item_list.append((time.time(), obj))
		self._item_set.add(obj)

	def pop(self):
		timestamp, obj = self._item_list.pop()
		self._item_set.remove(obj)
		return obj

	def rip(self, timestamp = None):
		if timestamp is None:
			index = len(self._item_list)
		else:
			index = bisect.bisect(self._item_list, (timestamp, None))

		if not index:
			return []

		data = map(lambda i: i[1], self._item_list[:index])
		del(self._item_list[:index])
		self._item_set.difference_update(data)

		return data
	
class TimeoutError(exceptions.Exception):
	pass

class QueueError(exceptions.Exception):
	pass

class QueueDeadlock(exceptions.Exception):
	pass

class Queue(object):
	'''
	Simple generic queue.
	'''
	def __init__(self, object_allocator, args, kwargs, **kw):
		#
		#
		#
		self._timeout   = kw.get('timeout', None)
		self._patient   = kw.get('patient', False)
		self._active    = True
		self._item_out  = 0
		self._item_list = ElementContainer()

		self._item_refs = {}
		self._item_save = {}
		self._item_time = {}

		self._out_total = 0L
		self._out_time  = 0
		self._out_max   = 0

		self._item_args   = args
		self._item_kwargs = kwargs
		self._item_alloc  = object_allocator
		#
		# priority list is presented lowest to highest
		#
		self._item_max    = {}
		self._item_queues = {}
		self._item_prios  = []

		size  = kw.get('size', 0)
		prios = kw.get('prios', [DEFAULT_PRIORITY])

		for index in range(len(prios)):
			if isinstance(prios[index], type(tuple())):
				size += prios[index][1]
				prio  = prios[index][0]
			else:
				prio  = prios[index]

			self._item_queues[prio] = coro.stats_cond(timeout = self._timeout)
			self._item_max[prio]    = size

			self._item_prios.insert(0, prio)

	def __repr__(self):
		state = self.state()

		entries = '(use: %d, max: %r, waiting: %r)' % \
			(state['out'], state['max'], state['wait'])
		return 'Queue: status <%s> entries %s' % (
			((self._active and 'on') or 'off'), entries,)
		
	def __alloc(self):
		o = self._item_alloc(
			*self._item_args, **self._item_kwargs)
		
		def dropped(ref):
			self._stop_info(self._item_save.pop(self._item_refs[ref], {}))

			self._item_out = self._item_out - 1
			del(self._item_refs[ref])
			self.wake_one()

		r = weakref.ref(o, dropped)
		self._item_refs[r] = id(o)

		return o

	def __dealloc(self, o):
		for r in weakref.getweakrefs(o):
			self._item_refs.pop(r, None)

	def _stop_info(self, data):
		'''_stop_info

		When an object is being returned to the queue, stop recording
		trace information in the thread that removed the object from
		the queue.
		'''
		thrd = coro._get_thread(data.get('thread', 0))
		if thrd is not None:
			thrd.trace(False)

		return data
		
	def _save_info(self, o):
		'''_save_info

		When an item is fetched from the queue, save information about
		the item request and the requester.
		'''
		data    = {'caller': pyinfo.rawstack(depth = 2), 'time':   time.time()}
		current = coro.current_thread()

		if current is not None:
			data.update({'thread': current.thread_id()})
			current.trace(True)

		self._item_save[id(o)] = data
		return o

	def _drop_info(self, o):
		'''_drop_info

		When an item is returned to the queue, release information about
		the original item request and the requester.
		'''
		return self._stop_info(self._item_save.pop(id(o), {}))

	def _dealloc(self, o):
		self.__dealloc(o)

	def _drop_all(self):
		'''_drop_all

		All currently allocated and queued (e.g. not checked out) are
		released and deallocated.
		'''
		while self._item_list:
			self._dealloc(self._item_list.pop())

	def timeout(self, *args):
		if not args:
			return self._timeout

		self._timeout = args[0]

		for cond in self._item_queues.values():
			cond.timeout = self._timeout

	def empty(self, prio):
		prio = (prio is None and self._item_prios[-1]) or prio
		return (not (self._item_out < self._item_max[prio]))

	def wake_all(self):
		for prio in self._item_prios:
			self._item_queues[prio].wake_all()

	def wake_one(self):
		for prio in self._item_prios:
			if len(self._item_queues[prio]):
				self._item_queues[prio].wake_one()
				break

	def wait_one(self, prio = None, timeout = None):
		prio = (prio is None and self._item_prios[-1]) or prio
		
		result = self._item_queues[prio].wait(timeout)
		if result is None:
			return (False, prio)     # wait timed out
		elif result:
			return (True, result[0]) # wait had a priority adjustment
		else:
			return (True, prio)      # wait completed successfully

	def bump(self, id, prio):
		for cond in self._item_queues.values():
			try:
				cond.wake(id, prio)
			except coro.CoroutineCondError:
				pass
			else:
				break

	def get(self, prio = None, poll = False):
		'''get

		Return an object from the queue, wait/yield if one is not available
		until it becomes available.

		Note: when the queue has a timeout defined, the return value
		      will be None when the timeout expires and no object has
			  become available.
		'''
		wait = not poll

		while wait and \
				  ((self.empty(prio) and self._active) or \
				   (self._patient and not self._active)):
			wait, prio = self.wait_one(prio)

		if self.empty(prio) or not self._active:
			return None

		if not self._item_list:
			self._item_list.add(self.__alloc())

		self._item_out  += 1
		self._out_total += 1

		return self._save_info(self._item_list.pop())

	def put(self, o):
		if o in self._item_list:
			raise QueueError('cannnot put object already in queue', o)

		timestamp = self._drop_info(o).get('time', None)
		if timestamp is not None:
			out_time = time.time() - timestamp

			self._out_time += out_time
			self._out_max   = max(out_time, self._out_max)
	
		self._item_out = self._item_out - 1

		if self._active:
			self._item_list.add(o)
		else:
			self._dealloc(o)

		self.wake_one()
		return None

	def active(self):
		return self._active

	def patient(self, v):
		self._patient = v

	def on(self, *args, **kwargs):
		self._active = True
		self.wake_all()

	def off(self, **kwargs):
		timeout = Timeout(kwargs.get('timeout', None))
		wait    = True
		#
		# It is possible to use the regular wait queue to wait for all
		# outstanding objects, since wake_all() clears out the entire
		# wait list synchronously and _active set to false prevents
		# anyone else from entering.
		#
		self._active = False
		self.wake_all()

		while wait and self._item_out and not self._active:
			wait, prio = self.wait_one(timeout = timeout())

		if not self._active:
			self._drop_all()

		return wait

	def trim(self, age):
		'''trim

		Any item in the queue which is older then age seconds gets reaped.
		'''
		count = 0

		for o in self._item_list.rip(time.time() - age):
			self._dealloc(o)
			count += 1

		return count

	def resize(self, size, prio = None):
		prio = (prio is None and self._item_prios[-1]) or prio
		self._item_max[prio] = size

	def size(self, prio = None):
		prio = (prio is None and self._item_prios[-1]) or prio
		return self._item_max[prio]

	def stats(self):
		qstats = {}
		for stat in map(lambda x: x.stats(), self._item_queues.values()):
			qstats['waiting'] = qstats.get('waiting', []) + stat['waiting']
			qstats['timeouts'] = qstats.get('timeouts', 0) + stat['timeouts']
			qstats['total'] = qstats.get('total', 0) + stat['total']
			qstats['waits'] = qstats.get('waits', 0) + stat['waits']

		if qstats['total']:
			qstats['average'] = qstats['total']/qstats['waits']
		else:
			qstats['average'] = 0

		qstats.update({
			'item_max': self._item_max.values(),
			'item_out': self._item_out,
			'requests': self._out_total,
			'out_max' : self._out_max,
			'pending' : self._item_save})
		qstats['pending'].sort()
		qstats['waiting'].sort()

		if  self._out_time:
			average = self._out_time/(self._out_total-self._item_out)
		else:
			average = 0

		qstats.update({'avg_pend': average})
		return qstats

	def state(self):
		item_max  = []
		item_wait = []
		item_left = []

		for index in range(len(self._item_prios)-1, -1, -1):
			prio = self._item_prios[index]

			item_max.append(self._item_max[prio])
			item_left.append(self._item_max[prio] - self._item_out)
			item_wait.append(len(self._item_queues[prio]))

		return {
			'out': self._item_out,
			'max': item_max,
			'in':  item_left,
			'wait': item_wait}

	def find(self):
		'''find

		Return information about each object that is currently checked out
		and therefore not in the queue.

		  caller - execution stack trace at the time the object was
		           removed from the queue
		  thread - coroutine ID of the thread executing at the time the
		           object was removed from the queue
		  trace  - current execution stack trace of the coroutine which
		           removed the object from the queue
		  time   - unix timestamp when the object was removed from the
		           queue.
		'''
		saved = self._item_save.copy()
		for oid, data in saved.items():
			thread = coro._get_thread(data['thread'])
			if thread is None:
				data.update({'trace': None})
			else:
				data.update({'trace': thread.where()})

		return saved


class SortedQueue(object):
	def __init__(
		self, object_allocator, args, kwargs, size = 0, timeout = None):

		self._timeout     = timeout
		self._item_args   = args
		self._item_kwargs = kwargs
		self._item_alloc  = object_allocator
		self._item_max    = size
		self._item_out    = 0

		self._out_total   = 0L
		self._out_time    = 0
		self._out_max     = 0

		self._item_list = []
		self._item_refs = {}
		self._item_time = {}
		self._item_cond = coro.stats_cond(timeout = self._timeout)

	def __repr__(self):
		state = self.state()
		entries = '(use: %d, max: %r, waiting: %r)' % \
				  (state['out'], state['max'], state['wait'])

		return 'SortedQueue: entries %s' % entries

	def __alloc(self):
		o = self._item_alloc(
			*self._item_args, **self._item_kwargs)

		def dropped(ref):
			self._item_out = self._item_out - 1
			del(self._item_refs[ref])
			self.wake_one()

		r = weakref.ref(o, dropped)
		self._item_refs[r] = id(o)

		return o

	def empty(self):
		return not (self._item_out < self._item_max)


	def wake_all(self):
		self._item_cond.wake_all()

	def wake_one(self):
		self._item_cond.wake_one()

	def wait_one(self):
		result = self._item_cond.wait()
		return not (result is None)

	def head(self, poll = False):
		return self._get(poll)

	def tail(self, poll = False):
		return self._get(poll, True)
	
	def _get(self, poll = False, tail = False):

		wait = not poll

		# wait for something to come in if we're not polling
		while wait and self.empty():
			wait = self.wait_one()

		if self.empty():
			return None

		if not self._item_list:
			if tail:
				return None
			self._item_list.append(self.__alloc())
			
		self._item_time[coro.current_id()] = time.time()
		self._item_out  += 1
		self._out_total += 1

		if tail:
			o = self._item_list[0]
			del(self._item_list[0])
		else:
			o = self._item_list.pop()

		return o

	def put(self, o):
		id = coro.current_id()
		if id in self._item_time:
			out_time = time.time() - self._item_time[id]
			self._out_time += out_time
			self._out_max = max(self._out_max, out_time)
			del(self._item_time[id])

		self._item_out -= 1
		bisect.insort(self._item_list, o)

		self.wake_one()
		return None

	def resize(self, size):
		self._item_max = size

	def size(self):
		return self._item_max

	def stats(self):
		qstats = {}
		qstats.update(self._item_cond.stats())

		qstats.update({
			'item_max' : self._item_max,
			'item_out' : self._item_out,
			'requests' : self._out_total,
			'out_max'  : self._out_max,
			'pending'  : self._item_time.values() })
		qstats['pending'].sort()
		qstats['waiting'].sort()

		if self._out_time:
			average = self._out_time/(self._out_total-self._item_out)
		else:
			average = 0

		qstats.update({'avg_pend' : average })
		return qstats

	def state(self):
		return { 'out'  : self._item_out,
				 'max'  : self._item_max,
				 'in'   : self._item_max - self._item_out,
				 'wait' : len(self._item_cond) }

class SafeQueue(Queue):
	'''SafeQueue

	Queue with protection against a single coro.thread requesting multiple
	objects and potentially deadlocking in the process.
	'''
	def __init__(self, *args, **kwargs):
		super(SafeQueue, self).__init__(*args, **kwargs)

		self._item_thrd = {}

	def _stop_info(self, data):
		data = super(SafeQueue, self)._stop_info(data)
		self._item_thrd.pop(data.get('thread', -1), None)

		return data
		
	def _save_info(self, o):
		o = super(SafeQueue, self)._save_info(o)
		t = self._item_save[id(o)].get('thread', -1)

		self._item_thrd[t] = id(o)
		return o

	def get(self, *args, **kwargs):
		'''get

		Return an object from the queue, wait/yield if one is not available
		until it becomes available.

		Note: when the queue has a timeout defined, the return value
		      will be None when the timeout expires and no object has
			  become available.

		QueueDeadlock raised if an attempt is made to get an object and
		the current thread already has fetched an object from the queue
		which has yet to be returned.
		'''
		if coro.current_id() in self._item_thrd:
			obj = self._item_thrd[coro.current_id()]
			raise QueueDeadlock(obj, self._item_save[obj])

		return super(SafeQueue, self).get(*args, **kwargs)

class ThreadQueue(Queue):
	def __init__(self, thread, args, size = 0,priorities = [DEFAULT_PRIORITY]):
		super(ThreadQueue, self).__init__(
			thread, (), {'args': args},	size = size, prios = priorities)

	def get(self, prio = None, poll = False):
		thrd = super(type(self), self).get(prio, poll)
		if thrd is not None:
			thrd.start()
		return thrd

class SimpleObject(object):
	def __init__(self, name):
		self.name = name
	def __repr__(self):
		return '<SimpleObject: %r>' % (self.name,)

class SimpleQueue(Queue):
	def __init__(self, name, size = 0, priorities = [DEFAULT_PRIORITY]):
		super(SimpleQueue, self).__init__(
			SimpleObject, (name, ), {}, size = size, prios = priorities)

class SMTPQueue(SafeQueue):

	def __init__(self, host_primary, host_secondary=None, priorities=None,
            size=0, optional={}, timeout=None, trace=False, **kw):
        priorities = priorities or [DEFAULT_PRIORITY]
        self.host_primary = host_primary
        self.host_secondary = host_secondary
		super(SMTPQueue, self).__init__(
			smtplib.SMTP, (), optional, size = size,
			prios = priorities, timeout = timeout, **kw)

	def get(self, *args, **kwargs):
		smtp = super(SMTPQueue, self).get(*args, **kwargs)
		if not getattr(smtp, 'sock', None):
			try:
				smtp.connect(self.host_primary)
			except socket.gaierror, gaie:
                if self.host_secondary:
                    smtp.connect(self.host_secondary)
                raise
		return smtp

	def _dealloc(self, o):
		super(SMTPQueue, self)._dealloc(o)
		o.close()

#
#
# end

