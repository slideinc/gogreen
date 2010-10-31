from gogreen import coro
from gogreen import emulate
from gogreen import purepydns

import prctl

import logging
import sys
import os
import getopt
import resource
import curses
import errno
import stat
import getpass
import socket

import logging
import logging.handlers

emulate.init()
purepydns.emulate()

LOG_SIZE_MAX  = 16*1024*1024
LOG_COUNT_MAX = 50

SERVER_MAX_FD = 32768

SRV_LOG_FRMT = '[%(name)s|%(coro)s|%(asctime)s|%(levelname)s] %(message)s'
LOGLEVELS = dict(
	CRITICAL=logging.CRITICAL, DEBUG=logging.DEBUG, ERROR=logging.ERROR,
	FATAL=logging.FATAL, INFO=logging.INFO, WARN=logging.WARN,
	WARNING=logging.WARNING,INSANE=5)

class GreenFormatter(object):
	def __init__(self, fmt = SRV_LOG_FRMT, width = 0):
		self.fmt = logging.Formatter(fmt)
		self.width = width
	def set_width(self, width):
		self.width = width
	def formatTime(self, record, datefmt=None):
		return self.fmt.formatTime(record, datefmt)
	def formatException(self, ei):
		return self.fmt.formatException(ei)
	def format(self, record):
		msg = self.fmt.format(record)
		if self.width:
			return msg[:self.width]
		else:
			return msg

def get_local_servers(config_map, user = None):
	"""get_local_servers returns a dictionary keyed by server ids of
	all configuration blocks from config_map for servers that run as
	the current user on the local machine.  The dictionary is keyed 
	list of configuration blocks all server configurations in
	config_map.
	"""
	local_srvs = {}
	user = user or getpass.getuser()
	name = socket.gethostname()
	for id, server in config_map.items():
		if server.get('host', 'localhost') in [name, 'localhost'] and \
			user == server.get('user', user):
			local_srvs[id] = server

	return local_srvs


def _lock(server):
	"""_lock attempt to bind and listen to server's lockport.
	Returns the listening socket object on success.
	Raises socket.error if the port is already locked.
	"""
	s = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
	s.set_reuse_addr()
	s.bind((server.get('bind_ip', ''), server['lockport']))
	s.listen(1024)

	return s

def lock_node(config_map, id=None):
	if id is None:
		for id, server in get_local_servers(config_map).items():
			try:
				s = _lock(server)
			except socket.error:
				pass
			else:
				server['lock']  = s
				server['total'] = len(config_map)
				server['id']    = id
				return server
	else:
		server = get_local_servers(config_map)[id]
		try:
			s = _lock(server)
		except socket.error:
			pass
		else:
			server['lock']  = s
			server['total'] = len(config_map)
			server['id']    = id
			return server

	return None # unused value


BASE_COMMAND_LINE_ARGS = [
	'help', 'fork', 'nowrap', 'logfile=', 'loglevel=', 'pidfile=',
	]

def usage(name, error = None):
	if error:
		print 'Error:', error
	print "  usage: %s [options]" % name

def main(
	serv_dict, exec_func,
	name = 'none', base_dir = '.', arg_list = [], defaults = {},
	prefork = None):

	log = coro.coroutine_logger(name)
	fmt = GreenFormatter()

	log.setLevel(logging.DEBUG)
	#
	# check for argument collisions
	#
	extra = set(map(lambda i: i.strip('='), arg_list))
	base  = set(map(lambda i: i.strip('='), BASE_COMMAND_LINE_ARGS))
	both  = tuple(extra & base)

	if both:
		raise AttributeError(
			'Collision between standard and extended command line arguments',
			both)

	progname = sys.argv[0]
	#
	# internal parameter defaults
	#
	max_fd   = defaults.get('max_fd', SERVER_MAX_FD)
	loglevel = 'INFO'
	logfile  = None
	pidfile  = None
	dofork   = False
	linewrap = True
	#
	# setup defaults for true/false parameters
	#
	parameters = {}

	for key in filter(lambda i: not i.endswith('='), arg_list):
		parameters[key] = False

	dirname  = os.path.dirname(os.path.abspath(progname))
	os.chdir(dirname)

	try:
		list, args = getopt.getopt(
			sys.argv[1:],
			[],
			BASE_COMMAND_LINE_ARGS + arg_list)
	except getopt.error, why:
		usage(progname, why)
		return None

	for (field, val) in list:
		field = field.strip('-')

		if field == 'help':
			usage(progname)
			return None
		elif field == 'nowrap':
			linewrap = False
		elif field == 'logfile':
			logfile = val
		elif field == 'loglevel':
			loglevel = val
		elif field == 'pidfile':
			pidfile = val
		elif field == 'fork':
			dofork = True
		elif field in extra:
			if field in arg_list:
				parameters[field] = True
			else:
				try:
					parameters[field] = int(val)
				except TypeError:
					parameters[field] = val

	# init
	here = lock_node(serv_dict)
	if here is None:
		return 128

	logdir = os.path.join(base_dir, here.get('logdir', 'logs'))
	try:
		value = os.stat(logdir)
	except OSError, e:
		if errno.ENOENT == e[0]:
			os.makedirs(logdir)
			os.chmod(logdir, stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
		else:
			print 'logdir lookup error: %r' % (e,)
			return 127

	if prefork is not None:
		parameters['prefork'] = prefork()

	if dofork:
		pid = os.fork()
		if pid:
			return 0

	try:
		resource.setrlimit(resource.RLIMIT_NOFILE, (max_fd, max_fd))
	except ValueError, e:
		if not os.getuid():
			print 'MAX FD error: %s, %d' % (e.args[0], max_fd)
	try:
		resource.setrlimit(resource.RLIMIT_CORE, (-1, -1))
	except ValueError, e:
		print 'CORE size error:', e

	if not os.getuid():
		os.setgid(1)
		os.setuid(1)

	try:
		prctl.prctl(prctl.DUMPABLE, 1)
	except (AttributeError, ValueError, prctl.PrctlError), e:
		print 'PRCTL DUMPABLE error:', e

	if pidfile:
		pidfile = logdir + '/' + pidfile
		try:
			fd = open(pidfile, 'w')
		except IOError, e:
			print 'IO error: %s' % (e.args[1])
			return None
		else:
			fd.write('%d' % os.getpid())
			fd.close()

	if logfile:
		logfile = logdir + '/' + logfile
		hndlr = logging.handlers.RotatingFileHandler(
			logfile, 'a', LOG_SIZE_MAX, LOG_COUNT_MAX)
		
		os.close(sys.stdin.fileno())
		os.close(sys.stdout.fileno())
		os.close(sys.stderr.fileno())
	else:
		if not linewrap:
			win = curses.initscr()
			height, width = win.getmaxyx()
			win = None
			curses.reset_shell_mode()
			curses.endwin()

			fmt.set_width(width)
			
		hndlr = logging.StreamHandler(sys.stdout)

	sys.stdout = coro.coroutine_stdout(log)
	sys.stderr = coro.coroutine_stderr(log)

	hndlr.setFormatter(fmt)
	log.addHandler(hndlr)
	loglevel = LOGLEVELS.get(loglevel, None)
	if loglevel is None:
		log.warn('Unknown logging level, using INFO: %r' % (loglevel, ))
		loglevel = logging.INFO

	max_fd = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
	log.info('uid: %d, gid: %d, max fd: %d' % (os.getuid(),os.getgid(),max_fd))

	result = exec_func(here, log, loglevel, logdir, **parameters)
	
	if result is not None:
		log.critical('Server exiting: %r' % (result,))
	else:
		log.critical('Server exit')

	return 0
