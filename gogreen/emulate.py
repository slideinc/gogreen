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

'''emulate

Various emulation/monkey-patch functions for functionality which we want
behaving differently in the coro environment. Generally that behaviour
is a coroutine yield and reschedule on code which would have otherwise
blocked the entire process.

In the past each module maintained individual emulation code, this becomes
unwieldy as the scope continues to expand and encourages functionality
creep.
'''
import time

import coro
import corocurl
import coromysql
import corofile
#
# save needed implementations.
#
original = {
	'sleep': time.sleep,
	}

def sleep(value):
	thrd = coro.current_thread()
	if thrd is None:
		original['sleep'](value)
	else:
		thrd.Yield(timeout = value)

def emulate_sleep():
	time.sleep = sleep


def init():
	'''all

	Enable emulation for all modules/code which can be emulated.

	NOTE: only code which works correctly in coro AND main can/should
	      be automatically initialized, the below emulations do not
	      fall into that category, they only work in a coroutine, which
	      is why the are here.	  
	'''
	coro.socket_emulate()
	corocurl.emulate()
	coromysql.emulate()
	corofile.emulate_popen2()
	#
	# auto-emulations
	#
	emulate_sleep()
#
# end..
