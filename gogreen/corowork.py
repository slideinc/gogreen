#!/usr/local/bin/python
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

"""corowork

Queue with worker threads to service entries.
"""

import coro
import coroqueue
import statistics

import exceptions
import time
import sys
import os

# same as btserv/cacches.py but not importable because of Logic circular import
DB_USAGE_MARKER = 'cache-db-usage'

class QueueError (exceptions.Exception):
	pass

class Worker(coro.Thread):
	def __init__(self, *args, **kwargs):
		super(Worker, self).__init__(*args, **kwargs)

		self._requests = kwargs['requests']
		self._filter   = kwargs['filter']
		self._prio     = kwargs.get('prio', 0)

		self._exit  = False
		self._drain = False

	def execute(self, *args, **kwargs):
		self.info('REQUEST args: %r kwargs: %r' % (args, kwargs))

	def run(self):
		self.info('Starting QueueWorker [%s]: %s' % (self._prio, self.__class__.__name__))

		while not self._exit:
			if self._drain:
				timeout = 0
			else:
				timeout = None

			try:
				args, kwargs, start = self._requests.pop(timeout = timeout)
			except coro.CoroutineCondWake:
				continue
			except coro.TimeoutError:
				break

			self.profile_clear()
			#
			# execution
			#
			self.execute(*args, **kwargs)
			#
			# record statistics
			#
			stamp = time.time()
			delta = stamp - start
			label = self._filter(args, kwargs)

			self.parent().record(stamp, delta, label)
		#
		# shutdown
		#
		self.info('Exiting QueueWorker')

	def shutdown(self):
		self._exit = True

	def drain(self):
		self._drain = True

	def requeue(self, *args, **kwargs):
		self._requests.push((args, kwargs, time.time()))

class CommandWorker(Worker):
	'''A worker thread that that expects command from a
	notify/rpc_call call to be part of the execute args. Attempts to
	lookup the method on this thread object and execute it.
	'''
	
	def execute(self, object, id, cmd, args, server = None, seq = None):
		handler = getattr(self, cmd, None)
		result = None
		if handler is not None and getattr(handler, 'command', 0):
			try:
				result = handler(args)
			except exceptions.Exception, e:
				self.traceback()
		if server is not None:
			server.rpc_response(seq, result)
			

class Server(coro.Thread):
	name = 'WorkQueueServer'

	def __init__(self, *args, **kwargs):
		super(Server, self).__init__(*args, **kwargs)

		self.work   = kwargs.get('worker', Worker)
		self.filter = kwargs.get('filter', lambda i,j: None)
		self.sizes  = kwargs.get('sizes') or [kwargs['size']]
		self.kwargs = kwargs
		self.stop   = False
		self.drain  = False

		self.rpcs    = {}
		self.cmdq    = []
		self.klist   = []
		self.workers = []

		self.identity = kwargs.get('identity', {})
		self.waiter   = coro.coroutine_cond()
		self.requests = []

		for size in self.sizes:
			self.requests.append(coro.coroutine_fifo())
			self.workers.append([])

		self.stats  = statistics.Recorder()
		self.wqsize = statistics.WQSizeRecorder()
		self.dbuse  = statistics.Recorder()

		self.wlimit = statistics.TopRecorder(threshold = 0.0)
		self.elimit = statistics.TopRecorder(threshold = 0.0)
		self.tlimit = statistics.TopRecorder(threshold = 0.0)
		self.rlimit = statistics.TopRecorder(threshold = 0)

		self.kwargs.update({
			'filter':   self.filter})

	def run(self):
		self.info('Starting %s.' % (self.name,))

		while not self.stop:
			#
			# spawn workers
			#
			while self.klist:
				work_klass, prio = self.klist.pop(0)
				work_kwargs = self.kwargs.copy()
				work_kwargs.update(
					{'requests' : self.requests[prio],
					 'prio'     : prio, })
				worker = work_klass(**work_kwargs)
				worker.start()
				self.workers[prio].append(worker)
			#
			# execute management commands
			#
			while self.cmdq:
				try:
					self.command_dispatch(*self.cmdq.pop(0))
				except:
					self.traceback()
			#
			# wait for something to do
			self.waiter.wait()

		self.info(
			'Stopping %s. (children: %d)' % (
				self.name,
				self.child_count()))

		self.command_clear()

		for child in self.child_list():
			child.drain()

		for r in self.requests:
			r.wake()
		self.child_wait()

	def shutdown(self, timeout = None):
		if not self.stop:
			self.stop = True
			self.waiter.wake_all()

		return self.join(timeout)

	def resize(self, size, prio = 0):
		if not size:
			return None

		if size == self.sizes[prio]:
			return None

		kill = max(0, len(self.workers[prio]) - size)
		while kill:
			kill -= 1
			work  = self.workers[prio].pop()
			work.drain()

		self.requests[prio].wake()
		self.sizes[prio] = size
			
	def spawn(self, prio = 0):

		if self.requests[prio].waiters():
			return None

		size = self.sizes[prio]
		if size > self._wcount(prio):
			self.klist.append((self.work, prio))
			self.waiter.wake_all()

	def request(self, *args, **kwargs):
		'''make a request for this work server. this method can take a prio
		kwarg:  when it is present, it specifies the maximum priority slot
		this request is made in. defaults to the first slot, 0.

		requests are put into the lowest possible slot until that slot is
		"full," here full meaning # of worker threads < size for priority slot.
		'''
		if self.stop:
			raise QueueError('Sevrer has been shutdown.')

		# find the priority to queue this request in to
		max_prio = min(kwargs.get('prio', 0), len(self.sizes))
		for prio in xrange(max_prio+1):
			if self.requests[prio].waiters():
				break
			self.workers[prio] = filter(
				lambda w: w.isAlive(), self.workers[prio])

			# if we got here it means nothing is waiting for something
			# to do at this prio. check to see if we can spawn.
			room = max(self.sizes[prio] - len(self.workers[prio]), 0)
			if not room:
				continue
			room -= len(filter(lambda i: i[1] == prio, self.klist))
			if room > 0:
				self.klist.append((self.work, prio))
				self.waiter.wake_all()
				break

		self.requests[prio].push((args, kwargs, time.time()))

	def record(self, current, elapse, label):
		self.stats.request(elapse, name = label, current = current)
		if coro.get_local(DB_USAGE_MARKER, False): 
			self.dbuse.request(elapse, name = label, current = current)
		self.wqsize.request(sum(map(len, self.requests)))

		data = (
			label, current, elapse, 
			coro.current_thread().total_time(),
			coro.current_thread().long_time(),
			coro.current_thread().resume_count())

		self.wlimit.save(data[2], data) # longest wall clock + queue wait times
		self.elimit.save(data[3], data) # longest execution times
		self.tlimit.save(data[4], data) # longest non-yield times
		self.rlimit.save(data[5], data) # most request yields
		
	def least_prio(self):
		'''return the smallest request priority that is not full. a full
		request priority is one that has created all of its worker threads
		'''
		for prio in xrange(len(self.sizes)):
			if self._wcount(prio) < self.sizes[prio]:
				break
		return prio

	def _wcount(self, prio):
		return len(filter(lambda t: t.isAlive(), self.workers[prio])) + \
			   len(filter(lambda k: k[1] == prio, self.klist))
	#
	# dispatch management/stats commands
	#
	def command_list(self):
		result = []

		for name in dir(self):
			handler = getattr(self, name, None)
			if not callable(handler):
				continue
			if getattr(handler, 'command', None) is None:
				continue
			name = name.split('_')
			if name[0] != 'object':
				continue
			name = '_'.join(name[1:])
			if not name:
				continue

			result.append(name)

		return result

	def command_clear(self):
		for seq in self.rpcs.keys():
			self.command_response(seq)

	def command_push(self, obj, id, cmd, args, seq, server):
		self.rpcs[seq] = server
		self.cmdq.append((obj, cmd, args, seq))

		self.waiter.wake_all()

	def command_response(self, seq, response = None):
		server = self.rpcs.pop(seq, None)
		if server is not None: server.rpc_response(seq, response)

	def command_dispatch(self, obj, cmd, args, seq):
		name = 'object_%s' % (cmd,)
		handler = getattr(self, name, None)
		response = self.identity.copy()

		if getattr(handler, 'command', None) is None:
			response.update({'rc': 1, 'msg': 'no handler: <%s>' % name})
			self.command_response(seq, response)
			return None

		try:
			result = handler(args)
		except exceptions.Exception, e:
			self.traceback()
			t,v,tb = coro.traceback_info()

			response.update({
				'rc': 1,
				'msg': 'Exception: [%s|%s]' % (t,v),
				'tb': tb})
		else:
			response.update({'rc': 0, 'result': result})

		self.command_response(seq, response)
		return None
#
# end..
