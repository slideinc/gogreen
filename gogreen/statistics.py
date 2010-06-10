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

'''statistics

Maintain second resolution statistics which can then be consolidated into
statistics for multiple time periods.
'''

import time
import bisect

QUEUE_PERIOD = [15, 60, 300, 600]
QUEUE_DEPTH  = QUEUE_PERIOD[-1]
HEAP_LIMIT   = 128

class Recorder(object):
	def __init__(self, depth = QUEUE_DEPTH, period = QUEUE_PERIOD):
		self._global = [{'timestamp': 0, 'elapse': 0, 'count': 0}]
		self._local  = {}
		self._depth  = depth
		self._period = period
		
	def _local_request(self, elapse, name, current):
		#
		# track average request time per second for a given period
		#
		record = self._local.setdefault(name, [])

		if not record or record[-1]['timestamp'] != current:
			record.append({'timestamp': current, 'elapse': 0, 'count': 0})

		record[-1]['count']  += 1
		record[-1]['elapse'] += int(elapse * 1000000)
		#
		# clear old entries
		#
		while len(record) > self._depth:
			del(record[0])

	def _global_request(self, elapse, current):
		#
		# track average request time per second for a given period
		#
		if self._global[-1]['timestamp'] != current:
			self._global.append({'timestamp': current,'elapse': 0,'count': 0})

		self._global[-1]['count']  += 1
		self._global[-1]['elapse'] += int(elapse * 1000000)
		#
		# clear old entries
		#
		while len(self._global) > self._depth:
			del(self._global[0])
	#
	# info API
	#
	def last(self):
		return self._global[-1]['timestamp']
	#
	# logging API
	#
	def request(self, elapse, name = 'none', current = None):
		if current is None:
			current = int(time.time())
		else:
			current = int(current)

		self._local_request(elapse, name, current)
		self._global_request(elapse, current)
	#
	# query API
	#
	def rate(self, current = None):
		current = current or int(time.time())
		results = []
		index   = 0

		timestamps, counts  = zip(*map(
			lambda x: (x['timestamp'], x['count']),
			self._global))

		for period in self._period[::-1]:
			index = bisect.bisect(timestamps, (current - period), index)
			results.append(sum(counts[index:])/period)

		return results[::-1]

	def details(self, current = None):
		current = current or int(time.time())
		results = {}

		for name, record in self._local.items():
			results[name] = []
			index = 0

			timestamps, counts, elapse = zip(*map(
				lambda x: (x['timestamp'], x['count'], x['elapse']),
				record))

			for period in self._period[::-1]:
				index = bisect.bisect(timestamps, (current - period), index)
				results[name].append({
					'count':   sum(counts[index:]),
					'elapse':  sum(elapse[index:]),
					'seconds': period})

			results[name].reverse()
		return results

	def averages(self, current = None):
		current = current or int(time.time())
		results = []
		index   = 0

		timestamps, counts, elapse  = zip(*map(
			lambda x: (x['timestamp'], x['count'], x['elapse']),
			self._global))

		for period in self._period[::-1]:
			index = bisect.bisect(timestamps, (current - period), index)
			reqs  = sum(counts[index:])

			results.append({
				'count':   reqs/period,
				'elapse':  (reqs and sum(elapse[index:])/reqs) or 0,
				'seconds': period})

		return results[::-1]


class RecorderHitRate(object):
	def __init__(self, depth = QUEUE_DEPTH, period = QUEUE_PERIOD):
		self._global = [(0, 0, 0)]
		self._depth  = depth
		self._period = period
	#
	# logging API
	#
	def request(self, hit = True, current = None):
		if current is None:
			current = int(time.time())
		else:
			current = int(current)

		pos = int(hit)
		neg = int(not hit)

		if self._global[-1][0] != current:
			self._global.append((current, pos, neg))
		else:
			current, oldpos, oldneg = self._global[-1]
			self._global[-1] = (current, oldpos + pos, oldneg + neg)

		while len(self._global) > self._depth:
			del(self._global[0])
	#
	# query API
	#
	def data(self):
		current = int(time.time())
		results = []
		index   = 0

		timelist, poslist, neglist = zip(*self._global)

		for period in self._period[::-1]:
			index = bisect.bisect(timelist, (current - period), index)

			pos = sum(poslist[index:])
			neg = sum(neglist[index:])

			results.append((pos, neg))

		return results[::-1]

	def rate(self):
		return map(
			lambda i: sum(i) and float(i[0])/sum(i) or None,
			self.data())

class WQSizeRecorder(object):

	def __init__(self, depth = QUEUE_DEPTH, period = QUEUE_PERIOD):
		self._global = [(0, 0, 0)]
		self._depth  = depth
		self._period = period

	# logging API
	def request(self, size, current = None):
		if current is None:
			current = int(time.time())
		else:
			current = int(current)

		size = int(size)

		if self._global[-1][0] != current:
			self._global.append((current, size, 1))
		else:
			current, oldsize, oldcnt = self._global[-1]
			self._global[-1] = (current, oldsize + size, oldcnt + 1)

		while len(self._global) > self._depth:
			del(self._global[0])

	# query api
	def sizes(self):
		current = int(time.time())
		results = []
		index   = 0

		timelist, sizelist, cntlist = zip(*self._global)

		for period in self._period[::-1]:
			index = bisect.bisect(timelist, (current - period), index)

			sizes = sizelist[index:]
			cnts  = cntlist[index:]
			results.append((sum(sizes), len(sizes)+sum(cnts)))

		return results[::-1]		

class TopRecorder(object):
	def __init__(self, depth = HEAP_LIMIT, threshold = None):
		self._heap  = [(0, None)]
		self._depth = depth
		self._hold  = threshold

	def fetch(self, limit = None):
		if limit:
			return self._heap[-limit:]
		else:
			return self._heap

	def save(self, value, data):
		if not value > self._hold:
			return None

		if value < self._heap[0][0]:
			return None

		bisect.insort(self._heap, (value, data))

		while len(self._heap) > self._depth:
			del(self._heap[0])
#
# end..
