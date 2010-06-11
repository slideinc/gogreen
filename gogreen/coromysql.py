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

# Note: this should be split into two modules, one which is unaware of
# the distinction between blocking and coroutine sockets.

# Strategies:
#   1) Simple; schedule at the socket level.
#      Just like the mysql client library, when a coroutine accesses
#        the mysql client object, it will automatically detach when
#        the socket gets EWOULDBLOCK.
#   2) Smart; schedule at the request level.
#      Use a separate coroutine to manage the mysql connection.
#      A client coroutine will resume when a response is available.
#   3) Sophisticated; schedule at the row level.
#      Allow a client coroutine to peel off rows one at a time.
#
#
# Currently I am trying to emulate MySQLmodule.c as closely as possible
# so it can be used as a drop-in replacement, from Mysqldb.py and up., If
# all the commands are not there, they are being added on an as needed
# basis.
#
# The one place where the stradegy takes a different route than the module
# is auto reconnects. The module does not perform auto reconnect, so it is
# left to higher layers like Mysqldb.py, which uses sleep. Unfortunatly
# regular sleep will block an entire process. (bad) So we are adding
# auto-reconnect to this module. -Libor 10/10/99
#

## dp 8/17/05
## Fool the mysql module by matching its version info.
version_info = (1, 2, 0, "final", 1)
#
# Use the original mysql c module for some things
#
import _mysql as origmysqlc

version_info = origmysqlc.version_info

import types
import exceptions
import operator
import math
import socket
import string
import struct
import sys
import sha
import array
import time

import coro

from coromysqlerr import *
#
# performance improvements used by the fast connection
#
try:
	import mysqlfuncs
except:
	mysqlfuncs = None

def log(msg):
	sys.stderr.write(msg + '\n')

MAX_RECONNECT_RETRY   = 1
RECONNECT_RETRY_GRAIN = 0.1
DEFAULT_RECV_SIZE     = 256 *1024
MYSQL_HEADER_SIZE     = 0x4

SEND_BUF_SIZE  = 256*1024
RECV_BUF_SIZE  = 256*1024

class MySQLError(exceptions.StandardError):
	pass
class Warning(exceptions.Warning, MySQLError):
	pass
class Error(MySQLError):
	pass
class InterfaceError(Error):
	pass
class DatabaseError(Error):
	pass
class DataError(DatabaseError):
	pass
class OperationalError(DatabaseError):
	pass
class IntegrityError(DatabaseError):
	pass
class InternalError(DatabaseError):
	pass
class ProgrammingError(DatabaseError):
	pass
class NotSupportedError(DatabaseError):
	pass

class error(MySQLError):
	pass
class NetworkError(MySQLError):
	pass

def error_to_except(merr):
	if not merr:
		return InterfaceError

	if merr > CR_MAX_ERROR:
		return InterfaceError

	if merr in [
		CR_COMMANDS_OUT_OF_SYNC,
		ER_DB_CREATE_EXISTS,
		ER_SYNTAX_ERROR,
		ER_PARSE_ERROR,
		ER_NO_SUCH_TABLE,
		ER_WRONG_DB_NAME,
		ER_WRONG_TABLE_NAME,
		ER_FIELD_SPECIFIED_TWICE,
		ER_INVALID_GROUP_FUNC_USE,
		ER_UNSUPPORTED_EXTENSION,
		ER_TABLE_MUST_HAVE_COLUMNS,
		ER_CANT_DO_THIS_DURING_AN_TRANSACTION]:
		return ProgrammingError

	if merr in [
		ER_DUP_ENTRY,
		ER_DUP_UNIQUE,
		ER_PRIMARY_CANT_HAVE_NULL]:
		return IntegrityError
  
	if merr in [ER_WARNING_NOT_COMPLETE_ROLLBACK]:
		return NotSupportedError

	if merr < 1000:
		return InternalError
	else:
		return OperationalError
 
# ===========================================================================
#						   Authentication
# ===========================================================================
#
# switching to 4.1 password/handshake.
#
# mysql-4.1.13a/libmysql/password.c
#
#  SERVER:  public_seed=create_random_string()
#           send(public_seed)
#
#  CLIENT:  recv(public_seed)
#           hash_stage1=sha1("password")
#           hash_stage2=sha1(hash_stage1)
#           reply=xor(hash_stage1, sha1(public_seed,hash_stage2)
#           send(reply)
#
#  SERVER:  recv(reply)
#           hash_stage1=xor(reply, sha1(public_seed,hash_stage2))
#           candidate_hash2=sha1(hash_stage1)
#           check(candidate_hash2==hash_stage2)

def scramble (message, password):
	hash_stage1 = sha.new(password).digest()
	hash_stage2 = sha.new(hash_stage1).digest()
	hash_stage2 = sha.new(message + hash_stage2).digest()

	reply = ''
	while hash_stage1 and hash_stage2:
		reply = reply + chr(ord(hash_stage1[0]) ^ ord(hash_stage2[0]))
		hash_stage1 = hash_stage1[1:]
		hash_stage2 = hash_stage2[1:]

	return reply
# ===========================================================================
#						   Packet Protocol
# ===========================================================================

def unpacket (p):
	# 3-byte length, one-byte packet number, followed by packet data
	a,b,c,s = map (ord, p[:4])
	l = a | (b << 8) | (c << 16)
	# s is a sequence number
  
	return l, s

def packet(data, s = 0):
	return struct.pack('<i', len(data) | s << 24) + data

def n_byte_num (data, n, pos=0):
	result = 0
	for i in range(n):
		result = result | (ord(data[pos+i])<<(8*i))

	return result

def decode_length (data):
	n = ord(data[0])
  
	if n < 251:
		return n, 1
	elif n == 251:
		return 0, 1
	elif n == 252:
		return n_byte_num (data, 2, 1), 3
	elif n == 253:
		# documentation says 32 bits, but wire decode shows 24 bits.
		# (there is enough padding on the wire for 32, but lets stick
		#  to 24...) Rollover to 254 happens at 24 bit boundry.
		return n_byte_num (data, 3, 1), 4
	elif n == 254:
		return n_byte_num (data, 8, 1), 9
	else:
		raise DataError, ('unexpected field length', n)

# used to generate the dumps below
def dump_hex (s):
	r1 = []
	r2 = []
  
	for ch in s:
		r1.append (' %02x' % ord(ch))
		if (ch in string.letters) or (ch in string.digits):
			r2.append ('  %c' % ch)
		else:
			r2.append ('   ')

	return string.join (r1, ''), string.join (r2, '')
# ===========================================================================
# generic utils
# ===========================================================================
def is_disconnect(reason):
	lower = string.lower(repr(reason.args))
	if string.find(lower, "lost connection") != -1:
		return 1

	if string.find(lower, "no connection") != -1:
		return 1

	if string.find(lower, "server has gone away") != -1:
		return 1

	return 0

def handle_error_sanely(cursor, errclass, errargs):
	raise

class connection(object):
	server_capabilities = 0

	def __init__ (self, **kwargs):
		self.username = kwargs.get('user',   '')
		self.password = kwargs.get('passwd', '')

		self.addresses = [(
			socket.AF_INET,
			(kwargs.get('host', '127.0.0.1'), kwargs.get('port', 3306)))]

		if kwargs.get('host') == 'localhost':
			self.addresses.append((socket.AF_UNIX, '/tmp/mysql.sock'))

		self._database = kwargs.get('db', '')
		self._init_cmd = kwargs.get('init_command', None)

		self._debug = kwargs.get('debug', 0) or (coro.current_id() < 0)

		self._connect_timeout = kwargs.get('connect_timeout', None)
		self._timeout         = kwargs.get('timeout', None)

		self._connected   = 0
		self._server_info = ''
		self._recv_buffer = ''

		self._rbuf = ''
		self._rlen = 0
		self._roff = 0
		self._rpac = []

		self._latest_fields = []
		self._latest_rows   = []
		self._nfields       = 0
		self._affected      = 0
		self._insert_id     = 0
		self._warning_count = 0
		self._message       = None

		self.socket = None
		self._lock  = 0
		self._max_reconnect = MAX_RECONNECT_RETRY

		if not self._debug:
			self._timer_cond = coro.coroutine_cond()
			self._lock_cond  = coro.coroutine_cond()

		self.converter = {}
		self.errorhandler = handle_error_sanely

		from MySQLdb import converters
		from weakref import proxy

		self.charset = self.character_set_name().split('_')[0]

		self.converter = converters.conversions.copy()
		self.converter[types.StringType]  = self.string_literal
		self.converter[types.UnicodeType] = self.unicode_literal
		#
		# preconverter set by upper layer, blech, use it for string types.
		#
		self._preconv = kwargs.get('conv', None)
		#
		# check switch for C implementation.
		#
		if kwargs.get('fast', False) and mysqlfuncs:
			self.read_packet = self._fast_read_packet
			self.cmd_query   = self._fast_cmd_query

		return None

	def __del__(self):
		self._close()

	def handle_error_sanely(self, self2, errclass, errargs):
		raise

	def sleep(self, *args, **kwargs):
		if self._debug:
			return apply(time.sleep, args, kwargs)
		else:
			return apply(self._timer_cond.wait, args, kwargs)

	def make_socket(self, *args, **kwargs):
		if self._debug:
			return apply(socket.socket, args, kwargs)
		else:
			return apply(coro.make_socket, args, kwargs)

	def lock_wait(self, *args, **kwargs):
		if not self._debug:
			return apply(self._lock_cond.wait, args, kwargs)

	def lock_wake(self, *args, **kwargs):
		if not self._debug:
			return apply(self._lock_cond.wake_one, args, kwargs)
		
	def lock_waiting(self):
		if not self._debug:
			return len(self._lock_cond)
		else:
			return 0

	def _lock_connection(self):

		while self._lock:
			self.lock_wait()

		self._lock = 1
		return None

	def _unlock_connection(self):
		waiting = self.lock_waiting()

		self._lock = 0
		self.lock_wake()
		#
		# yield to give someone else a chance
		#
		if waiting:
			self.sleep(0.0)

	def _close(self):
		if self.socket is not None:
			self.socket.close()
			
		self._server_info = ''
		self._connected   = 0

		self._rbuf = ''
		self._rlen = 0
		self._roff = 0
		self._rpac = []

		self.address = None
		self.socket  = None

	def close(self):
		if self._connected:
			self.cmd_quit()

		self._close()

	def _connect(self):
		self.socket  = None
		self.address = None

		sock = None

		for family, addr in self.addresses:
			try:
				sock = self.make_socket(family, socket.SOCK_STREAM)
				sock.settimeout(self._connect_timeout)

				sock.connect(addr)
			except (socket.error, coro.TimeoutError):
				sock = None
			else:
				break

		if sock is None:
			raise NetworkError(
				CR_CONNECTION_ERROR,
				'Can not connect to MySQL server')

		self.address = addr
		self.socket  = sock
		self.socket.settimeout(self._timeout)
		self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
		self.socket.setsockopt(
			socket.SOL_SOCKET,
			socket.SO_SNDBUF,
			SEND_BUF_SIZE)
		self.socket.setsockopt(
			socket.SOL_SOCKET,
			socket.SO_RCVBUF,
			RECV_BUF_SIZE)

		self._rbuf = ''
		self._rlen = 0
		self._roff = 0
		self._rpac = []

		return None

	def recv(self):
		try:
			data = self.socket.recv(DEFAULT_RECV_SIZE)
		except (socket.error, coro.TimeoutError):
			data = ''

		if not data:
			raise NetworkError, (
				CR_SERVER_LOST, 'Lost connection to MySQL server during query')

		if self._rbuf:
			self._rbuf += data
		else:
			self._rbuf  = data

	def write (self, data):
		while data:
			try:
				n = self.socket.send(data)
			except (socket.error, coro.TimeoutError):
				raise NetworkError, (
					CR_SERVER_LOST,	'No connection to MySQL server')
		
			if not n:
				raise NetworkError, (
					CR_SERVER_LOST,	'Lost connection to MySQL server')

			data = data[n:]

	def send_packet(self, data, sequence=0):
		self.write(packet(data, sequence))

	def read_packet(self):
		while not self._rpac:
			self.recv()
			self.rrip()

		return self._rpac.pop(0)

	def rrip(self):
		off = 0

		while len(self._rbuf) > off:
			#
			# header
			#
			if (len(self._rbuf) - off) < MYSQL_HEADER_SIZE:
				break
			#
			# 3-byte length, one-byte packet number, followed by packet data
			#
			val = struct.unpack('<i', self._rbuf[off:off + 4])[0]
			size, seq = (val & 0xFFFFFF, val >> 24)
			off += MYSQL_HEADER_SIZE
			#
			# body
			#
			if (len(self._rbuf) - off) < size:
				off -= MYSQL_HEADER_SIZE
				break
			#
			# now we have at least one packet
			#
			self._rpac.append(self._rbuf[off:off + size])
			off += size

		self._rbuf  = self._rbuf[off:]

	def _login (self):
		data = self.read_packet()
		#
		# unpack the greeting
		protocol_version = ord(data[:1])
		data = data[1:]

		eos = string.find (data, '\00')
		mysql_version = data[:eos]
		data = data[eos + 1:]
		
		thread_id = n_byte_num(data, 4)
		data = data[4:]
		
		challenge = data[:8]
		data = data[8:]
		data = data[1:] # filler

		capability = data[:2]
		data = data[2:]

		server_language = data[:1]
		data = data[1:]

		server_status = data[:2]
		data = data[2:]
		data = data[13:] # filler

		challenge = challenge + data[:12]
		data = data[12:]

		if self.password:
			auth = scramble(challenge, self.password)
		else:
			auth = ''
		# 2 bytes of client_capability
		# 3 bytes of max_allowed_packet
		# no idea what they are

		lp = (
			'\x0d\x86\x03\x00' +  # Client Flags
			'\x00\x00\x00\x01' +      # Max Packet Size
			server_language +         # Charset Number
			('\00' * 23) +            # Filler
			self.username + '\00' +   # username
			chr(len(auth)) + auth +   # scramble buffer
			self._database + '\00')   # database name

		# seems to require a sequence number of one
		self.send_packet (lp, 1)
		#
		# read the response, which will check for errors
		#
		response_tuple = self.read_reply_header()
		if response_tuple != (0, 0, 0, 0):
			raise InternalError, 'unknow header response: <%s>' % \
				(repr(response_tuple))

		self._server_info = mysql_version

		#
		# mark that we are now connected
		#
		return None

	def check_connection(self):
		if self._connected:
			return None

		self._connect()
		self._login()

		if self._database is not None:
			self.cmd_use(self._database)

		if self._init_cmd is not None:
			self.cmd_query(self._init_cmd)

		self._connected = 1

	def command(self, command_type, command):
		q = chr(decode_db_cmds[command_type]) + command
		self.send_packet (q, 0)

	def unpack_len (self, d):
		if ord(d[0]) > 250:
			fl, sc = decode_length (d)
		else:
			fl, sc = (ord(d[0]), 1)

		return fl, sc
  
	def unpack_data (self, d):
		r = []
		while len(d):
			fl, sc = self.unpack_len(d)

			r.append (d[sc:sc+fl])
			d = d[sc+fl:]

		return r

	def unpack_field (self, d):
		r = []

		for name, size in decode_pos_field:
			if not len(d):
				raise InternalError, 'No data before all fields processed.'

			if size == -1:
				fl, sc = self.unpack_len(d)

				r.append(d[sc:sc+fl])
				d = d[sc+fl:]
			else:
				r.append(d[:size])
				d = d[size:]

		# 4.1 has an optional default field
		if len(d):
			fl, sc = self.unpack_len(d)

			r.append(d[sc:sc+fl])
			d = d[sc+fl:]

		if len(d):
			raise InternalError, 'Still have data, but finished parsing.'

		return r

	def unpack_int(self, data_str):

		if len(data_str) > 4:
			raise TypeError, 'data too long for an int32: <%d>' % len(data_str)

		value = 0

		while len(data_str):

			i = ord(data_str[len(data_str)-1])
			data_str = data_str[:len(data_str)-1]

			value = value + (i << (8 * len(data_str)))

		return value

	def read_field_packet(self):
		#
		# handle the generic portion for all types of result packets
		#
		data = self.read_packet()
		if data[0] < '\xfe':
			return data

		if data[0] == '\xfe':
			# more data in 4.1:
			# 2 bytes - warning_count
			# 2 bytes - server_status
			return None
		#
		# data[0] == 0xFF
		#
		error_num = ord(data[1]) + (ord(data[2]) << 8)
		error_msg = data[3:]

		raise error_to_except(error_num), (error_num, error_msg)

	def read_reply_header(self):
		#
		# read in the reply header and return the results.
		#
		data = self.read_field_packet()
		if data is None:
			raise InternalError, 'unexpected EOF in header'

		rows_in_set = 0
		affected_rows = 0
		insert_id = 0
		warning_count = 0
		server_status = None

		rows_in_set, move = decode_length(data)
		data = data[move:]

		if len(data):

			affected_rows, move = decode_length(data)
			data = data[move:]
			insert_id, move     = decode_length(data)
			data = data[move:]

			server_status, warning_count = struct.unpack('<2sh', data[:4])
			data = data[4:]
		#
		# save the remainder as the message
		#
		self._message = data

		return rows_in_set, affected_rows, insert_id, warning_count
	#
	# Internal mysql client requests to get raw data from db (cmd_*)
	#
	def cmd_use(self, database):
		self.command('init_db', database)

		rows, affected, insert_id, warning_count = self.read_reply_header()

		if rows != 0 or affected != 0 or insert_id != 0:
			msg = 'unexpected header: <%d:%d:%d>' % (rows, affected, insert_id)
			raise InternalError, msg

		self._database = database
		return None
  
	def cmd_query(self, query):
		#print 'coro mysql query: "%s"' % (repr(query))
		self.command('query', query)
		#
		# read in the header
		#
		(self._nfields,
		 self._affected,
		 self._insert_id,
		 self._warning_count) = self.read_reply_header()

		self._latest_fields = []
		self._latest_rows   = []

		if not self._nfields:
			return 0 #statement([], [], affected, insert_id)

		decoders = range(self._nfields)
		fields = []
		i = 0

		# read column data.
		while 1:
			data = self.read_field_packet()
			if data is None:
				break

			field = self.unpack_field (data)
			type  = ord(field[decode_field_pos['type']])
			flags = struct.unpack('H', field[decode_field_pos['flags']])[0]

			field[decode_field_pos['type']]  = type
			field[decode_field_pos['flags']] = flags

			decoders[i] = (
				decode_type_map[type],
				flags,
				self._preconv.get(type, None))

			fields.append(field)

			i = i + 1

		if len(fields) != self._nfields:
			raise InternalError, "number of fields did not match"

		# read rows
		rows = []
		field_range = range(self._nfields)

		while 1:
			data = self.read_field_packet()
			if data is None:
				break

			row = self.unpack_data (data)
			#
			# cycle through all fields in the row appling decoders
			#
			for i in field_range:
				decode, flags, preconv = decoders[i]
				#
				# find preconverter if it exists and is a sequence. This
				# is field dependent
				#
				if operator.isSequenceType(preconv):
					func = None

					for mask, p in preconv:
						if mask is None or mask & flags:
							func = p
							break
				else:
					func = preconv
				#
				# call decoder
				#
				row[i] = decode(row[i], flags, func)
			#
			# save entire decoded row
			#
		
			rows.append(row)

		self._latest_fields = fields
		self._latest_rows = rows
		return len(rows) # statement(fields, rows)

	def cmd_quit(self):
		self.command('quit', '')
		#
		# no reply!
		#
		return None
  
	def cmd_shutdown(self):
		self.command('shutdown', '')

		data = self.read_field_packet()
		print "shutdown: data: <%s>" % (repr(data))

		return None
  
	def cmd_drop(self, db_name):
		self.command('drop_db', db_name)

		(self._nfields,
		 self._affected,
		 self._insert_id,
		 self._warning_count) = self.read_reply_header()
		return None
  
	def cmd_listfields(self, cmd):
		self.command('field_list', cmd)

		rows = []
		#
		# read data line until we get 255 which is error or 254 which is
		# end of data ( I think :-)
		#
		while 1:

			data = self.read_field_packet()
			if data is None:
				return rows

			row = self.unpack_data(data)

			table_name = row[0]
			field_name = row[1]
			field_size = self.unpack_int(row[2])
			field_type = decode_type_names[ord(row[3])]
			field_flag = self.unpack_int(row[4])
			field_val  = row[5]

			flag = ''

			if field_flag & decode_flag_value['pri_key']:
				flag = flag + decode_flag_name['pri_key']
			if field_flag & decode_flag_value['not_null']:
				flag = flag + ' ' + decode_flag_name['not_null']
			if field_flag & decode_flag_value['unique_key']:
				flag = flag + ' ' + decode_flag_name['unique_key']
			if field_flag & decode_flag_value['multiple_key']:
				flag = flag + ' ' + decode_flag_name['multiple_key']
			if field_flag & decode_flag_value['auto']:
				flag = flag + ' ' + decode_flag_name['auto']
			#
			# for some reason we do not pass back the default value (row[5])
			#
			rows.append([field_name, table_name, field_type,
						 field_size, flag])

		return None

	def cmd_create(self, name):
		self.command('create_db', name)
		#
		# response
		#
		(self._nfields,
		 self._affected,
		 self._insert_id,
		 self._warning_count) = self.read_reply_header()
		return None

	def reconnect_retry_set(self, value = MAX_RECONNECT_RETRY):
		'''reconnect_retry_set

		Set the max reconnect retry count. Once a transaction has begun
		this must be set to 0 to ensure correctness.
		'''
		self._max_reconnect = value

	def _reconnect_retry(self):
		return self._max_reconnect

	def _execute_with_retry(self, method_name, args = ()):
		method = getattr(self, method_name)
		retry_count = 0
		error = None
		#
		# lock down the connection while we are performing a query.
		#
		self._lock_connection()

		try:
			while not (retry_count > self._reconnect_retry()):
				try:
					self.check_connection()
				except NetworkError, e:
					raise error_to_except(e[0]), e.args

				try:
					retval = apply(method, args)
				except NetworkError, e:
					self._close()
					error = e
				except IntegrityError:
					raise
				except ProgrammingError:
					raise
				except:
					self._close()
					raise
				else:
					error = None
					break
				#
				# network error, sleep and retry
				#
				sleep_time = retry_count * RECONNECT_RETRY_GRAIN
				retry_count += 1

				log('<%r> lost connection, sleeping <%0.1f>' % (
					self.address, sleep_time))

				self.sleep(sleep_time)
		finally:
			#
			# unlock the connection ( I must be retarded for not having this in
			# a finally clause. I was loosing the connect thread, and everyone
			# else was frozen on the lock.)
			#
			self._unlock_connection()

		if error is not None:
			raise error_to_except(error[0]), error.args

		return retval
	#
	# MySQL module compatibility, properly wraps raw client requests,
	# to format the return types.
	#
	# use_result option is currently not implemented, if anyone has the
	# time, please add support for it. Libor 4/2/00
	#
	def selectdb(self, database, use_result = 0):
		return self._execute_with_retry('cmd_use', (database,))

	def query (self, q, use_result = 0):
		return self._execute_with_retry('cmd_query', (q,))
	
	def listtables (self, wildcard = None):
		if wildcard is None:
			cmd = "show tables"
		else:
			cmd = "show tables like '%s'" % (wildcard)

		o = self._execute_with_retry('cmd_query', (cmd,))
		return o.fetchrows()

	def listfields (self, table_name, wildcard = None):
		if wildcard is None:
			cmd = "%s\000\000" % (table_name)
		else:
			cmd = "%s\000%s\000" % (table_name, wildcard)

		return self._execute_with_retry('cmd_listfields', (cmd,))
  
	def drop(self, database, use_result = 0):
		return self._execute_with_retry('cmd_drop', (database,))

	def create(self, db_name, use_result = 0):
		return self._execute_with_retry('cmd_create', (db_name,))

	def character_set_name(self):
		return 'utf8'
  
	def next_result(self):
		return -1

	def store_result(self):
		value = statement(
			self,
			self._latest_fields,
			self._latest_rows,
			self._affected,
			self._insert_id)
		return value
  
	def affected_rows(self):
		return len(self._latest_rows) or self._affected

	def insert_id(self):
		return self._insert_id

	def info(self):
		return self._message

	def warning_count(self):
		# return self._warning_count
		return 0

	def escape(self, o, converter):
		return origmysqlc.escape(o, self.converter)

	def string_literal(self, obj, dummy=None):
		return origmysqlc.string_literal(obj)

	def unicode_literal(self, obj, dummy=None):
		return self.string_literal(obj.encode(self.charset))

	def get_server_info(self):
		try:
			self.check_connection()
		except NetworkError, e:
			raise error_to_except(e[0]), e.args
		return self._server_info
	#
	# Fast Option
	#
	def _fast_read_packet(self):
		while not self._rpac:
			self.recv()
			self._rpac = mysqlfuncs.rip_packets(self._rbuf)

		return self._rpac.pop(0)
	
	def _fast_cmd_query(self, query):
		self.command('query', query)
		#
		# read in the header
		#
		(self._nfields,
		 self._affected,
		 self._insert_id,
		 self._warning_count) = self.read_reply_header()

		self._latest_fields = []
		self._latest_rows   = []

		if not self._nfields:
			return 0 # statement([], [], affected, insert_id)

		fields = []
		rows   = []
		#
		# read column data.
		#
		while 1:
			data = self.read_field_packet()
			if data is None:
				break

			fields.append(mysqlfuncs.unpack_field(data))

		if len(fields) != self._nfields:
			raise InternalError, "number of fields did not match"
		#
		# read rows
		#
		while 1:
			data = self.read_field_packet()
			if data is None:
				break

			rows.append(mysqlfuncs.unpack_data(data, fields))
			#
			# cycle through all fields in the row appling decoders
			#
			# for i in xrange(self._nfields):
			#	field   = fields[i]
			#	decode  = decode_type_map[field[9]]
			#	flags   = field[10]
			#	preconv = self._preconv.get(field[9])
			#	#
			#	# find preconverter if it exists and is a sequence. This
			#	# is field dependent
			#	#
			#	if operator.isSequenceType(preconv):
			#		func = None

			#		for mask, p in preconv:
			#			if mask is None or mask & flags:
			#				func = p
			#				break
			#	else:
			#		func = preconv
			#	#
			#	# call decoder
			#	#
			#	row[i] = decode(row[i], flags, func)

		self._latest_fields = fields
		self._latest_rows = rows
		return len(rows) # statement(fields, rows)


class statement(object):
	def __init__ (self, db, fields, rows, affected_rows = -1, insert_id = 0):
		self._fields = fields
		self._flags = []
		self._rows = rows

		if affected_rows > 0:
			self._affected_rows = affected_rows
		else:
			self._affected_rows = len(rows)

		self._index = 0
		self._insert_id = insert_id

	# =======================================================================
	# internal methods
	# =======================================================================
	def _fetchone (self):
		if self._index <  len(self._rows):
			result = self._rows[self._index]
			self._index = self._index + 1
		else:
			result = []

		return result
		
	def _fetchmany (self, size):
		result = self._rows[self._index:self._index + size]
		self._index = self._index + len(result)

		return result

	def _fetchall (self):
		result = self._rows[self._index:]
		self._index = self._index + len(result)

		return result
	# =======================================================================
	# external methods
	# =======================================================================
	def affectedrows (self):
		return self._affected_rows
  
	def numrows (self):
		return len(self._rows)

	def numfields(self):
		return len(self._fields)
  
	def fields (self):
		# raw format:
		# table, fieldname, ??? (flags?), datatype
		# ['groupmap', 'gid', '\013\000\000', '\003', '\013B\000']
		# MySQL returns
		# ['gid', 'groupmap', 'long', 11, 'pri notnull auto_inc mkey']

		result = []
		for field in self._fields:
			flag_list = []
			flag_value = struct.unpack(
				'H', field[decode_field_pos['flags']])[0]

			for value, name in decode_flag.items():
				if 0 < value & flag_value:
					flag_list.append(name)

			self._flags = flag_list

			type = field[decode_field_pos['type']]

			result.append(
				[field[decode_field_pos['table']],
				 field[decode_field_pos['name']],
				 decode_type_names[ord(type)],
				 string.join(flag_list)])

		return result
  
	def fetch_row(self, size, fetch_type):
		#The rows are formatted according to how:
		#  0 -- tuples (default)
		#  1 -- dictionaries, key=column or table.column if duplicated
		#  2 -- dictionaries, key=table.column
		if fetch_type:
			raise InternalError, 'unsupported row result type: %d' % fetch_type

		if size:
			value = self._fetchmany(size)
		else:
			value = self._fetchall()

		return tuple(map(lambda x: tuple(x), value))
  
	def fetchrows(self, size = 0):
		if size:
			return self._fetchmany(size)
		else:
			return self._fetchall()

	# [{'groupmap.podid': 2,
	#   'groupmap.listname': 'medusa',
	#   'groupmap.active': 'y',
	#   'groupmap.gid': 116225,
	#   'groupmap.locked': 'n'}]
	def fetchdict (self, size = 0):
		result = []
		keys = []

		for field in self._fields:
			keys.append('%s.%s' % (field[decode_field_pos['table']],
								   field[decode_field_pos['name']]))

		range_len_keys = range(len(keys))
		for row in self.fetchrows(size):

			d = {}
			for j in range_len_keys:
				d[keys[j]] = row[j]

			result.append(d)

		return result

	def insert_id (self):
		# i have no idea what this is
		return self._insert_id

	def describe(self):
		# http://www.python.org/peps/pep-0249.html
		# (name, type_code, display_size, internal_size, precision, scale,
		#  null_ok). The first two items (name and type_code) are mandatory.
		return tuple(map(
			lambda x: (x[4], x[9], None, None, None, None, None),
			self._fields))

	def field_flags(self):
		return self._flags

# ======================================================================
# decoding MySQL data types
# ======================================================================

def _is_flag_notnull(f):
	return 0 < (f & decode_flag_value['not_null'])

_is_flag_notnull = lambda f: bool(f & decode_flag_value['not_null'])
_is_flag_notnull = lambda f: bool(f & 1)

# decode string as int, unless string is empty.
def _null_int(s, f, p = None):
	if len(s):
		return int(s)
	else:
		return None

def _null_float(s, f, p = None):
	if len(s):
		return float(s)
	else:
		return None

# decode string as long, unless string is empty.
def _null_long(s, f, p = None):
	if len(s):
		return long(s)
	else:
		return None

def _array_str(s, f, p = None):
	return array.array('c', s)

def _null_str(s, f, p = None):
	if s:
		if p:
			return p(s)
		else:
			return s
		
	if _is_flag_notnull(f):
		return ''
	else:
		return None

# by default leave as a string
decode_type_map = [_null_str] * 256
decode_type_names = ['unknown'] * 256

# Many of these are not correct!  Note especially
# the time/date types... If you want to write a real decoder
# for any of these, just replace 'str' with your function.

for code, cast, name in (
	(0x00,	_null_int,	'decimal'),
	(0x01,	_null_int,	'tiny'),
	(0x02,	_null_int,	'short'),
	(0x03,	_null_long,	'long'),
	(0x04,	_null_float,	'float'),
	(0x05,	_null_float,	'double'),
	(0x06,	_null_str,	'null'),
	(0x07,	_null_str,	'timestamp'),
	(0x08,	_null_long,	'longlong'),
	(0x09,	_null_int,	'int24'),
	(0x0A,	_null_str,	'date'),       # unsure handling...
	(0x0B,	_null_str,	'time'),       # unsure handling...
	(0x0C,	_null_str,	'datetime'),   # unsure handling...
	(0x0D,	_null_str,	'year'),       # unsure handling...
	(0x0E,	_null_str,	'newdate'),    # unsure handling...
	(0x0F,	_null_str,	'varchar'),    # unsure handling... MySQL 5.0
	(0x10,	_null_str,	'bit'),        # unsure handling... MySQL 5.0
	(0xF6,	_null_str,	'newdecimal'), # unsure handling... MySQL 5.0
	(0xF7,	_null_str,	'enum'),       # unsure handling...
	(0xF8,	_null_str,	'set'),        # unsure handling...
	(0xF9,	_null_str,	'tiny_blob'),
	(0xFA,	_null_str,	'medium_blob'),
	(0xFB,	_null_str,	'long_blob'),
	(0xFC,	_null_str,	'blob'),
	(0xFD,	_null_str,	'var_string'), # in the C code it is VAR_STRING
	(0xFE,	_null_str,	'string'),
	(0xFF,	_null_str,	'geometry')    # unsure handling...
	):
	decode_type_map[code] = cast
	decode_type_names[code] = name
#
# we need flag mappings also
#
decode_flag_value = {}
decode_flag_name  = {}
decode_flag       = {}

for value, flag, name in (
	(1,     'not_null',      'notnull'),  # Field can not be NULL
	(2,     'pri_key',       'pri'),      # Field is part of a primary key
	(4,     'unique_key',    'ukey'),     # Field is part of a unique key
	(8,     'multiple_key',  'mkey'),     # Field is part of a key
	(16,    'blob',          'unused'),   # Field is a blob
	(32,    'unsigned',      'unused'),   # Field is unsigned
	(64,    'zerofill',      'unused'),   # Field is zerofill
	(128,   'binary',        'unused'),
	(256,   'enum',          'unused'),   # field is an enum
	(512,   'auto',          'auto_inc'), # field is a autoincrement field
	(1024,  'timestamp',     'unused'),   # Field is a timestamp
	(2048,  'set',           'unused'),   # field is a set
	(16384, 'part_key',      'unused'),   # Intern; Part of some key
	(32768, 'group',         'unused'),   # Intern: Group field
	(65536, 'unique',        'unused')    # Intern: Used by sql_yacc
	):
	decode_flag_value[flag] = value
	decode_flag_name[flag]  = name
	decode_flag[value] = name
#
# database commands
#
decode_db_cmds = {}

for value, name in (
	(0,  'sleep'),
	(1,  'quit'),
	(2,  'init_db'),
	(3,  'query'),
	(4,  'field_list'),
	(5,  'create_db'),
	(6,  'drop_db'),
	(7,  'refresh'),
	(8,  'shutdown'),
	(9,  'statistics'),
	(10, 'process_info'),
	(11, 'connect'),
	(12, 'process_kill'),
	(13, 'debug')
	):
	decode_db_cmds[name] = value
#
# database commands
#
decode_db_cmds = {}

for value, name in (
	(0,  'sleep'),
	(1,  'quit'),
	(2,  'init_db'),
	(3,  'query'),
	(4,  'field_list'),
	(5,  'create_db'),
	(6,  'drop_db'),
	(7,  'refresh'),
	(8,  'shutdown'),
	(9,  'statistics'),
	(10, 'process_info'),
	(11, 'connect'),
	(12, 'process_kill'),
	(13, 'debug')
	):
	decode_db_cmds[name] = value
#
# Mysql 4.1 fields
#
decode_field_pos = {}
decode_pos_field = []

for pos, size, name in (
	(0,  -1, 'catalog'),
	(1,  -1, 'db'),
	(2,  -1, 'table'),
	(3,  -1, 'org_table'),
	(4,  -1, 'name'),
	(5,  -1, 'org_name'),
	(6,   1, '(filler 1)'),
	(7,   2, 'charset'),
	(8,   4, 'length'),
	(9,   1, 'type'),
	(10,  2, 'flags'),
	(11,  1, 'decimals'),
	(12,  1, '(filler 2)')
	):
	decode_pos_field.append((name, size))
	decode_field_pos[name] = pos
## ======================================================================
##
## SMR - borrowed from daGADFLY.py, moved dict 'constant' out of
##       function definition.
##
quote_for_escape = {'\0': '\\0', "'": "\\'", '"': '\\"', '\\': '\\\\'}

import types

def escape(s):
	quote = quote_for_escape
	if type(s) == types.IntType:
		return str(s)
	if s == None:
		return ""
	if type(s) == types.StringType:
		r = range(len(s))
		r.reverse()		 # iterate backwards, so as not to destroy indexing

		for i in r:
			if quote.has_key(s[i]):
				s = s[:i] + quote[s[i]] + s[i+1:]
		return s
  
	log(s)
	log (type(s))
	raise MySQLError
#
# MySQL module compatibility
#
def connect (host, user, passwd, db="", timeout=None, connect_timeout=None):
	conn = connection(
		user, passwd, (host, 3306), debug=0, timeout=timeout,
		connect_timeout=connect_timeout)

	# I found that this is the best way to maximize the number of ultimately
	# successful requests if many threads (>50) are running. - martinb 99/11/03
	try:
		conn.check_connection()
	except InternalError, msg:
		pass
	return conn
#
# emulate standard MySQL calls and errors.
#
_emulate_list = [
	('MySQL',    None),
	('_mysql',   None),
	('MySQLdb', 'MySQLError'),
	('MySQLdb', 'Warning'),
	('MySQLdb', 'Error'),
	('MySQLdb', 'InterfaceError'),
	('MySQLdb', 'DatabaseError'),
	('MySQLdb', 'DataError'),
	('MySQLdb', 'OperationalError'),
	('MySQLdb', 'IntegrityError'),
	('MySQLdb', 'InternalError'),
	('MySQLdb', 'ProgrammingError'),
	('MySQLdb', 'NotSupportedError'),
]
_original_emulate = {}
def emulate():
	"have this module pretend to be the real MySQL module"
	import MySQLdb

	# save _emulate_list
	for module, attr in _emulate_list:
		if not attr:
			_original_emulate.setdefault((module, attr), sys.modules.get(module))
			continue
		_original_emulate.setdefault(
			(module, attr),
			getattr(sys.modules[module], attr, None))

	sys.modules['MySQL'] = sys.modules[__name__]
	sys.modules['_mysql'] = sys.modules[__name__]

	sys.modules['MySQLdb'].MySQLError        = MySQLError
	sys.modules['MySQLdb'].Warning           = Warning
	sys.modules['MySQLdb'].Error             = Error
	sys.modules['MySQLdb'].InterfaceError    = InterfaceError
	sys.modules['MySQLdb'].DatabaseError     = DatabaseError
	sys.modules['MySQLdb'].DataError         = DataError
	sys.modules['MySQLdb'].OperationalError  = OperationalError
	sys.modules['MySQLdb'].IntegrityError    = IntegrityError
	sys.modules['MySQLdb'].InternalError     = InternalError
	sys.modules['MySQLdb'].ProgrammingError  = ProgrammingError
	sys.modules['MySQLdb'].NotSupportedError = NotSupportedError

def reverse():
	for module, attr in _emulate_list:
		if not attr:
			if _original_emulate.get((module, attr)):
				sys.modules[module] = _original_emulate[(module, attr)]
			continue
		if _original_emulate.get((module, attr)):
			setattr(sys.modules[module], attr, _original_emulate[(module, attr)])

def test ():
	c = connection ('rushing', 'fnord', ('127.0.0.1', 3306))
	print 'connecting...'
	c.connect()
	print 'logging in...'
	c.login()
	print c
	c.cmd_use ('mysql')
	print c.cmd_query ('select * from host')
	c.cmd_quit()

if __name__ == '__main__':
	for i in range(10):
		coro.spawn (test)
	coro.event_loop (30.0)
#
# - connection is analogous to DBH in MySQLmodule.c, and statment is
#   analogous to STH in MySQLmodule.c
# - DBH is the database handler, and STH is the statment handler,
# - Here are the methods that the MySQLmodule.c implements, and if they
#   are at least attempted here in coromysql
#
# DBH:
#
#    "selectdb"       - yes
#    "do"             - no
#    "query"          - yes
#    "listdbs"        - no
#    "listtables"     - yes
#    "listfields"     - yes
#    "listprocesses"  - no
#    "create"         - yes
#    "stat"           - no
#    "clientinfo"     - no
#    "hostinfo"       - no
#    "serverinfo"     - no
#    "protoinfo"      - no
#    "drop"           - yes
#    "reload"         - no
#    "insert_id"      - no
#    "close"          - yes
#    "shutdown"       - no
#
# STH:
#
#    "fields"         - yes
#    "fetchrows"      - yes
#    "fetchdict"      - yes
#    "seek"           - no
#    "numrows"        - yes
#    "numfields"      - yes
#    "eof"            - no
#    "affectedrows"   - yes
#    "insert_id"      - yes 


## dp new stuff

string_literal = origmysqlc.string_literal
escape_sequence = origmysqlc.escape_sequence
escape_dict = origmysqlc.escape_dict
NULL = origmysqlc.NULL
get_client_info = lambda: '5.0.67-kb8'
#
# end...

