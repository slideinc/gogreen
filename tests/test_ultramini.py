"""test_ultramini

Tests for the ultramini web framework.
"""

import cgi
import cStringIO
import logging
import sys
import types
import unittest
import warnings

from gogreen import coro
from gogreen import ultramini
from gogreen import corohttpd



warnings.filterwarnings(
	'ignore', category=ultramini.LocationDeprecationWarning)


########################################################################
## Testcase helpers
########################################################################


class CoroTestCase(unittest.TestCase):
	def setUp(self):
		super(CoroTestCase, self).setUp()
		coro.socket_emulate()
	
	def tearDown(self):
		super(CoroTestCase, self).tearDown()
		coro.socket_reverse()


DEFAULT = object()
FAILURE = Exception("Test failed.")
PASSED = object()

class FakeServer(object):
	def __init__(self, condition):
		self.access = self
		self.condition = condition

	def request_started(self, req, cur=None):
		pass

	def info(self, bytes):
		coro.current_thread().info(bytes)

	def request_ended(self, req, code, start_time, push_time, rsize, wsize):
		self.condition.wake_all()

	def record_counts(self, *args):
		pass


class FakeSocket(object):
	def __init__(self, bytes):
		self.bytes = bytes
		self.pushed = ''

	def recv(self, num):
		if num > len(self.bytes):
			num = len(self.bytes)
		torecv = self.bytes[:num]
		self.bytes = self.bytes[num:]
		if not torecv:
			raise corohttpd.ConnectionClosed()
		return torecv

	read = recv

	def send(self, bytes):
		self.pushed += bytes
		return len(bytes)

	def close(self):
		pass

	def getsockname(self):
		return "<fake>"
	
	def shutdown(self, *args, **kwargs):
		pass


def sendreq(site, bytes):
	result = []
	def actually_send():
		socket = FakeSocket(bytes)
		waiter = coro.coroutine_cond()
		server = FakeServer(waiter)
		log = coro.coroutine_logger('tests')
		output = cStringIO.StringIO()
		log.addHandler(logging.StreamHandler(output))
		http = corohttpd.HttpProtocol(
			args=(socket, ('<fake>', 0), server, [site]), log=log)
		http.start()
		waiter.wait(0.01)
		if socket.pushed.startswith('HTTP/1.1 500'):
			output.seek(0)
			print output.read()
		result.append(socket.pushed)

	coro.spawn(actually_send)
	coro.event_loop()
	rv = result[0].split('\r\n\r\n', 1)
	if len(rv) != 2:
		rv = ('', '')
	return rv


class FakeRequest(corohttpd.HttpRequest):
	def __init__(self, query, site, method,  uri, headers, path, body, resource):
		self.site = site

		# These should be private but they aren't
		self._query = query
		self._path = path

		# These are private
		self._headers_ = headers
		self._method_ = method
		self._body_ = body
		self._uri_ = uri

		self.pushed = ''
		self._response_ = 200

		self._outgoing_headers_ = {}

		if resource is None:
			resource = ultramini.Resource()
		self.resource = resource

		self.convert = ultramini.convert

	def method(self):
		return self._method_

	def uri(self):
		return self._uri_

	def get_headers(self):
		return self._headers_

	def get_header(self, name, default=None):
		return self._headers_.get(name, default)

	def push(self, stuff):
		self.pushed += stuff

	def set_header(self, key, value):
		self._outgoing_headers_[key] = value

	def connection(self):
		return FakeSocket(self._body_)

	def response(self, code):
		self._response_ = code

	def result(self):
		return self._response_, self.pushed, self._outgoing_headers_

	def get_field_storage(self):
		if self._method_ == 'get':
			environ = dict(
				QUERY_STRING=self._query,
				REQUEST_METHOD='get')
			fp = None
		else:
			environ = dict(
				CONTENT_TYPE='application/x-www-form-urlencoded',
				REQUEST_METHOD='post')
			fp = cStringIO.StringIO(self._body_)
		
		fs = cgi.FieldStorage(
			fp, self.get_headers(),
			environ=environ)
		return fs

	def write(self, stuff):
		self.push(ultramini.convert(self, stuff))


def loc(
	query='', site=None, method='get', resource=None, 
	uri='http://fakeuri', body='', path='/',
	headers=None):
	if headers is None:
		headers = {
			'content-type': 'application/x-www-form-urlencoded'}
	if method == 'post':
		headers['content-length'] = str(len(body))
	return FakeRequest(
		query, site, method, uri, headers, path, body, resource)


def write(stuff, resource=None):
	l = loc(resource=resource)
	l.write(stuff)
	return l.pushed




########################################################################
## post_* and get_* argument converter tests
########################################################################

class TestThing(CoroTestCase):
	def runTest(self):
		mything = ultramini.Thing('')
		otherthing = mything.foo
		assert otherthing.name == 'foo'
		thirdthing = mything('bar', 'baz')
		assert thirdthing.name == 'bar'
		assert thirdthing.default == 'baz'

class TestLocation(CoroTestCase):
	def runTest(self):
		l = loc()

		@ultramini.arguments(ultramini.LOCATION)
		def some_function(foo):
			assert foo == l

		ultramini.convertFunction(l, some_function)

class TestRequest(CoroTestCase):
	def runTest(self):
		req = loc()

		@ultramini.arguments(ultramini.REQUEST)
		def some_function(foo):
			assert foo == req

		ultramini.convertFunction(req, some_function)


class TestSite(CoroTestCase):
	def runTest(self):
		@ultramini.arguments(ultramini.SITE)
		def some_function(foo):
			assert foo == PASSED

		ultramini.convertFunction(
			loc(site=PASSED), some_function)

class TestResource(CoroTestCase):
	def runTest(self):
		@ultramini.arguments(ultramini.RESOURCE)
		def some_function(foo):
			assert foo == PASSED

		ultramini.convertFunction(
			loc(resource=PASSED), some_function)

class TestMethod(CoroTestCase):
	def runTest(self):
		@ultramini.arguments(ultramini.METHOD)
		def some_function(foo):
			assert foo == "GLORP"

		ultramini.convertFunction(
			loc(method="GLORP"), some_function)

class TestURL(CoroTestCase):
	def runTest(self):
		@ultramini.arguments(ultramini.URI)
		def some_function(foo):
			assert foo == "/foo.gif"

		ultramini.convertFunction(
			loc(uri="/foo.gif"), some_function)

class TestQuery(CoroTestCase):
	def runTest(self):
		l = loc("foo=bar")
		@ultramini.arguments(ultramini.QUERY.foo)
		def some_function(foo):
			assert foo == 'bar'

		ultramini.convertFunction(l, some_function)

class TestMissingQuery(CoroTestCase):
	def runTest(self):
		l = loc("foo=bar")

		@ultramini.arguments(ultramini.QUERY('bar', DEFAULT))
		def another(bar):
			assert bar == DEFAULT

		ultramini.convertFunction(l, another)
			  
class TestQueries(CoroTestCase):
	def runTest(self):
		l = loc("foo=bar&foo=baz")

		@ultramini.arguments(ultramini.QUERIES.foo)
		def some_function(foo):
			foo = tuple(foo)
			assert foo == ('bar', 'baz')

		ultramini.convertFunction(l, some_function)

class TestMissingQueries(CoroTestCase):
	def runTest(self):
		l = loc("foo=bar&foo=baz")

		@ultramini.arguments(ultramini.QUERIES('bar', DEFAULT))
		def another(foo):
			assert foo == DEFAULT

		ultramini.convertFunction(l, another)

class TestArg(CoroTestCase):
	def runTest(self):
		l = loc(body='foo=bar', method='post')

		@ultramini.arguments(ultramini.ARG.foo)
		def some_function(foo):
			assert foo == 'bar'

		ultramini.convertFunction(l, some_function)

class TestMissingArg(CoroTestCase):
	def runTest(self):
		l = loc(body='foo=bar', method='post')

		@ultramini.arguments(ultramini.ARG('bar', DEFAULT))
		def another(foo):
			assert foo == DEFAULT

		ultramini.convertFunction(l, another)


class TestArgs(CoroTestCase):
	def runTest(self):
		l = loc(body='foo=bar&foo=baz', method='post')

		@ultramini.arguments(ultramini.ARGS.foo)
		def some_function(foo):
			assert foo == ['bar', 'baz']

		ultramini.convertFunction(l, some_function)

class TestMissingArgs(CoroTestCase):
	def runTest(self):
		l = loc(body='foo=bar&foo=baz', method='post')

		@ultramini.arguments(ultramini.ARGS('bar', DEFAULT))
		def another(foo):
			assert foo == DEFAULT

		ultramini.convertFunction(l, another)

class TestCookie(CoroTestCase):
	def runTest(self):
		l = loc(headers=dict(Cookie="foo=bar"))

		@ultramini.arguments(ultramini.COOKIE.foo)
		def some_function(foo):
			assert foo == "bar"

		ultramini.convertFunction(l, some_function)

class TestMissingCookie(CoroTestCase):
	def runTest(self):
		l = loc(headers=dict(Cookie="foo=bar"))

		@ultramini.arguments(ultramini.COOKIE('bar', DEFAULT))
		def another(foo):
			assert foo == DEFAULT

		ultramini.convertFunction(l, another)


class TestNoCookies(CoroTestCase):
	def runTest(self):
		@ultramini.arguments(ultramini.COOKIE('missing', DEFAULT))
		def another(foo):
			assert foo == DEFAULT

		ultramini.convertFunction(loc(), another)

class TestHeader(CoroTestCase):
	def runTest(self):
		l = loc(headers=dict(foo='bar'))

		@ultramini.arguments(ultramini.HEADER.foo)
		def some_function(foo):
			assert foo == 'bar'

		ultramini.convertFunction(l, some_function)

class TestQuotedQuery(CoroTestCase):
	def runTest(self):
		l = loc(query="channel=462280&amp;ver=3&amp;p=run")

		@ultramini.arguments(ultramini.QUERY.channel)
		def some_function(channel):
			assert channel == "462280"

		ultramini.convertFunction(l, some_function)

class TestLiteralArgument(CoroTestCase):
	def runTest(self):
		@ultramini.arguments(1234)
		def some_function(channel):
			assert channel == 1234

		ultramini.convertFunction(loc(), some_function)


########################################################################
## rendering converter tests: things which are contained within a
## resource's template attribute
########################################################################


class ConverterTests(CoroTestCase):
	def test_unicode(self):
		the_string = u'\u00bfHabla espa\u00f1ol?'
		assert  write(the_string) == the_string.encode('utf8')


	def test_list(self):
		assert write(['abc', 1, 2, 3]) == 'abc123'


	def test_tuple(self):
		assert write(('abc', 1, 2, 3)) == 'abc123'


	def test_generator(self):
		def the_generator():
			yield 'abc'
			yield 1
			yield 2
			yield 3

		assert write(the_generator) == 'abc123'


	def test_function(self):
		def the_function():
			return "abc123"

		assert write(the_function) == 'abc123'


	def test_method(self):
		class Foo(object):
			def __init__(self):
				self.bar = 'abc123'

			def baz(self):
				return self.bar

		f = Foo()
		assert write(f.baz) == f.bar


	def test_unbound_method(self):
		class Foo(object):
			@ultramini.arguments(ultramini.RESOURCE)
			def method(self):
				return 'asdf'

		assert write(Foo.method, Foo()) == 'asdf'


	def test_int(self):
		the_number = 12345
		assert write(the_number) == str(the_number)


	def test_float(self):
		the_number = 3.1415
		assert write(the_number) == str(the_number)


	def test_long(self):
		the_number = 100000L * 100000L
		assert write(the_number) == str(the_number)


	def test_show(self):
		class Foo(object):
			def show_bar(self):
				return 'hello'

		assert write(ultramini.show.bar, Foo()) == 'hello'


	def test_show_arguments(self):
		class Foo(object):
			def show_bar(self, foo):
				return foo

		assert write(ultramini.show.bar('foo'), Foo()) == 'foo'


	def test_iter_show(self):
		assert list(iter(ultramini.show.bar)) == []

	def test_string_quoting(self):
		assert write("<&>") == "&lt;&amp;&gt;"

	def test_pattern_omitted(self):
		assert write(
			ultramini.img[ultramini.span(pattern='foo')]) == '<img></img>'


	def test_stan_show_special(self):
		class Foo(object):
			template = ultramini.img[ultramini.span(show=ultramini.show.bar)]

			def show_bar(self):
				return "baz"

		written = write(Foo.template, Foo())
		assert written == '<img>baz</img>'


	def test_mutate_stan(self):
		foo = ultramini.img()
		foo(src='bar')

		assert write(foo) == '<img src="bar"></img>'

	def test_register_converter(self):
		class Blarg(object):
			pass

		def convert_blarg(request, blarg):
			return 'blarg.'

		ultramini.registerConverter(Blarg, convert_blarg)

		written = write(Blarg())
		assert written == 'blarg.'


	def test_no_converter(self):
		class Wrong(object):
			pass

		self.failUnlessRaises(RuntimeError, write, Wrong())


########################################################################
## Cast converter tests
########################################################################

class TypeConversionTests(CoroTestCase):
	def test_quoted_query_converter(self):
		l = loc(query="channel=462280&amp;ver=3&amp;p=run")

		@ultramini.annotate(channel=int)
		def some_function(channel):
			assert channel == 462280

		ultramini.applyWithTypeConversion(some_function, l)


	def test_int_converter(self):
		l = loc(query="channel=462280")

		@ultramini.annotate(channel=int)
		def some_function(channel):
			assert channel == 462280

		ultramini.applyWithTypeConversion(some_function, l)


	def test_failing_int_converter(self):
		l = loc(query="channel=asdf")

		@ultramini.annotate(channel=int)
		def somefunc(channel):
			raise FAILURE

		self.failUnlessRaises(
			TypeError, ultramini.applyWithTypeConversion, somefunc, l)


	def test_float_converter(self):
		l = loc(query="pi=3.14")

		@ultramini.annotate(pi=float)
		def somefunc(pi):
			assert pi == 3.14
			
		ultramini.applyWithTypeConversion(somefunc, l)


	def test_failing_float_converter(self):
		l = loc(query='pi=asdf')

		@ultramini.annotate(pi=float)
		def somefunc(pi):
			raise FAILURE

		self.failUnlessRaises(
			TypeError, ultramini.applyWithTypeConversion, somefunc, l)


	def test_list_converter(self):
		l = loc(query="foo=bar&foo=baz")

		@ultramini.annotate(foo=[str])
		def somefunc(foo):
			assert foo == ['bar', 'baz']

		ultramini.applyWithTypeConversion(somefunc, l)


	def test_list_single(self):
		l = loc(query="foo=bar")

		@ultramini.annotate(foo=[str])
		def somefunc(foo):
			assert foo == ['bar']

		ultramini.applyWithTypeConversion(somefunc, l)


	def test_list_fail(self):
		l = loc(query="foo=bar")

		@ultramini.annotate(foo=[int])
		def somefunc(foo):
			raise FAILURE

		self.failUnlessRaises(
			TypeError, ultramini.applyWithTypeConversion, somefunc, l)


	def test_tuple_converter(self):
		l = loc(query="foo=1&foo=hello&foo=3.1415")

		@ultramini.annotate(foo=(int, str, float))
		def somefunc(foo):
			(fi, fs, ff) = foo
			assert fi == 1
			assert fs == 'hello'
			assert ff == 3.1415

		ultramini.applyWithTypeConversion(somefunc, l)


	def test_dict_converter(self):
		l = loc(query="foo.bar=1&foo.baz=asdf")

		@ultramini.annotate(foo=dict(bar=int, baz=str))
		def somefunc(foo):
			assert isinstance(foo, dict)
			assert foo['bar'] == 1
			assert foo['baz'] == 'asdf'

		ultramini.applyWithTypeConversion(somefunc, l)


	def test_str_converter(self):
		l = loc(query='foo=foo')

		@ultramini.annotate(foo=str)
		def somefunc(foo):
			assert foo == 'foo'

		ultramini.applyWithTypeConversion(somefunc, l)


	def test_unicode_converter(self):
		l = loc(query=u'foo=\u00bfHabla%20espa\u00f1ol?'.encode('utf8'))

		@ultramini.annotate(foo=unicode)
		def somefunc(foo):
			assert foo == u'\u00bfHabla espa\u00f1ol?'

		ultramini.applyWithTypeConversion(somefunc, l)


	def test_function_converter(self):
		l = loc(query='foo=something')

		def converter_function(request, value):
			return "else"

		@ultramini.annotate(foo=converter_function)
		def somefunc(foo):
			assert foo == 'else'

		ultramini.applyWithTypeConversion(somefunc, l)


	def test_annotate_return_converter(self):
		def converter_function(something):
			assert something == 'wrong'
			return "right"

		@ultramini.annotate(converter_function)
		def somefunc():
			return 'wrong'

		success, result = ultramini.applyWithTypeConversion(somefunc, loc())
		# applyWithTypeConversion should return (hasConverted, result)
		# so we want (True, 'right')
		assert success, ('Failed to properly convert')
		assert result == 'right', (result, 'Incorrect return')


	def test_no__incoming_converter(self):
		class Wrong(object):
			pass

		@ultramini.annotate(foo=Wrong)
		def somefunc(foo):
			raise FAILED

		self.failUnlessRaises(
			TypeError, ultramini.applyWithTypeConversion, somefunc, loc(
				query='foo=bar'))

	def test_input_and_output_converter(self):
		def caps_in(resource, request, parameters):
			for k, v in parameters.iteritems():
				parameters[k] = unicode(v).upper()
			return parameters

		def lower_out(result):
			return unicode(result).lower()

		@ultramini.annotate(lower_out, caps_in, foo=str)
		def somefunc(foo):
			if not foo == 'WORLD':
				return 'World was supposed to be caps :('
			return 'HeLLo %s' % foo

		l = loc(query='foo=world')
		success, result = ultramini.applyWithTypeConversion(somefunc, l)
		assert success
		assert result == 'hello world', (success, result)

	def test_chained_input_converters(self):
		def intify(resource, request, params):
			for k in params.iterkeys():
				params[k] = int(params[k])
			return params

		def subtract_five(resource, request, params):
			for k in params.iterkeys():
				params[k] = params[k] - 5
			return params

		@ultramini.annotate(unicode, (intify, subtract_five), foo=int)
		def somefunc(foo):
			return foo

		l = loc(query='foo=10')
		success, result = ultramini.applyWithTypeConversion(somefunc, l)
		assert success
		assert result == u'5', (success, result)

	def test_custom_converter(self):
		old = ultramini.converters[str]
		try:
			ultramini.registerConverter(str, lambda r, s: ultramini.xml(s))
			@ultramini.annotate(str)
			def handler():
				return '<strong/>'

			l = loc()
			success, result = ultramini.applyWithTypeConversion(handler, l)
			result = ultramini.convert(l, result)
			assert success
			assert result == '<strong/>', (success, result)
		finally:
			ultramini.registerConverter(str, old)



########################################################################
## Test url traversal
########################################################################


class URLTraversalTests(CoroTestCase):
	def find_resource(self, path, root):
		req = loc(path=path)
		ultramini.SiteMap(root).match(req)
		return req.resource


	def test_root(self):
		root = ultramini.Resource()
		result = self.find_resource('/', root)
		assert result == root


	def test_child(self):
		root = ultramini.Resource()
		child = root.child_foo = ultramini.Resource()
		result = self.find_resource('/foo', root)
		assert result == child


	def test_findChild(self):
		child = ultramini.Resource()
		class Foo(ultramini.Resource):
			def findChild(self, req, segments):
				return child, segments, ()

		root = Foo()

		assert self.find_resource('/foobar', root) == child
		assert self.find_resource('/', root) == child
		assert self.find_resource('/asdfasdf/asdfasdf/asdfasdf', root) == child


	def test_childFactory(self):
		class Child(ultramini.Resource):
			def __init__(self, name):
				self.name = name

		class Root(ultramini.Resource):
			def childFactory(self, req, name):
				return Child(name)

		root = Root()

		assert self.find_resource('/foo', root).name == 'foo'
		assert self.find_resource('/bar', root).name == 'bar'
		assert self.find_resource('/baz', root).name == 'baz'


	def test_willHandle(self):
		child = ultramini.Resource()

		class Root(ultramini.Resource):
			def willHandle(self, req):
				return child

			def childFactory(self, req, segments):
				return self, segments, ()

		assert self.find_resource('/', Root()) == child


########################################################################
## Test handling
########################################################################


class HandlingTests(CoroTestCase):
	def handle_resource(self, req, root):
		ultramini.SiteMap(root).match(req)
		req.resource.handle(req)
		return req.result()


	def test_handle_get(self):
		simple = ultramini.Resource()
		simple.template = 'hello'

		code, result, headers = self.handle_resource(loc(), simple)
		assert code == 200
		assert result == 'hello'


	def test_handle_post(self):
		simple = ultramini.Resource()
		simple.template = 'hello'

		## Assert that we redirect
		code, result, headers = self.handle_resource(
			loc(method='post', headers={'content-length': '0'}), simple)
		assert code == 303, (code, headers, result)
		assert result == ''
		assert headers['Location'] == loc().uri()


	def test_handle_get_method(self):
		class Simple(ultramini.Resource):
			def get_default(self):
				return 'hello'

		code, result, headers = self.handle_resource(loc(), Simple())
		assert code == 200
		assert result == 'hello'


	def test_handle_get_custom(self):
		class Simple(ultramini.Resource):
			def get_default(self):
				return 'fail'

			def get_foo(self):
				return 'pass'

		code, result, headers = self.handle_resource(loc(query='get=foo'), Simple())
		assert code == 200
		assert result == 'pass'


	def test_handle_post_method(self):
		class Simple(ultramini.Resource):
			def post_default(self):
				return '/foo'

		code, result, headers = self.handle_resource(
			loc(method='post'), Simple())
		assert code == 303
		assert result == ''
		assert headers['Location'] == '/foo'


	def test_handle_post_custom(self):
		class Simple(ultramini.Resource):
			def post_default(self):
				return '/wrong'

			def post_foo(self):
				return '/foo'

		code, result, headers = self.handle_resource(
			loc(method='post', body='post=foo'), Simple())
		assert code == 303
		assert result == ''
		assert headers['Location'] == '/foo'


	def test_missing_methods(self):
		expected = (
			"<html><body>No handler for method 'GURFLE' present.</body></html>")

		code, result, headers = self.handle_resource(
			loc(method='GURFLE'), ultramini.Resource())

		assert code == 200
		assert result == expected


########################################################################
## Test that we interact with corohttpd properly
########################################################################


GET_TEMPLATE = """GET %s HTTP/1.1\r
Host: localhost\r
\r
"""


POST_TEMPLATE = """POST %s HTTP/1.1\r
Host: localhost\r
Content-length: %s\r
\r
%s"""


class CorohttpdTests(CoroTestCase):
	system = True # Marking these system-tests since they mostly suck
	def test_simple_http(self):
		root = ultramini.Resource()
		root.template = 'hello'
		site = ultramini.SiteMap(root)
		headers, result = sendreq(site, GET_TEMPLATE % ('/', ))

		assert result == 'hello', (site, result, headers)


	def test_simple_not_found(self):
		root = ultramini.Resource()
		site = ultramini.SiteMap(root)
		headers, result = sendreq(
			site, GET_TEMPLATE % ('/asdfkjasdfhakshdfkhaqe/asdfhas/a.dhf', ))
		assert headers.startswith("HTTP/1.1 404"), ('Fail', headers, result)


	def test_query_parameter(self):
		class Simple(ultramini.Resource):
			@ultramini.annotate(foo=str)
			def get_default(self, foo):
				assert foo == 'bar'

		root = Simple()
		headers, result = sendreq(
			ultramini.SiteMap(root), GET_TEMPLATE % ('/?foo=bar', ))

		assert headers.startswith("HTTP/1.1 200")

		
	def test_header(self):
		class Simple(ultramini.Resource):
			@ultramini.arguments(ultramini.HEADER.host)
			def host(self, foo):
				assert foo == 'localhost'
				return "ok"

			template = host

		headers, result = sendreq(
			ultramini.SiteMap(Simple()), GET_TEMPLATE % ('/', ))

		assert result == 'ok'


	def test_continuation_header(self):
		class Simple(ultramini.Resource):
			@ultramini.arguments(ultramini.HEADER.continuation)
			def continuation(self, foo):
				assert foo == "foo bar baz\n foo bar baz"
				return "ok"

			template = continuation

		headers, result = sendreq(
			ultramini.SiteMap(Simple()), """GET / HTTP/1.1\r
Host: localhost\r
Continuation: foo bar baz\r
 foo bar baz\r
Another-Header: 1234-asdf\r
\r
	""")
		self.assertEqual(result, 'ok')


	def test_outgoing_header(self):
		class Simple(ultramini.Resource):
			@ultramini.annotate(req=ultramini.REQUEST)
			def get_default(self, req):
				req.set_header('Some-header', 'Some-value')
				assert req.has_key('Some-header')
				assert req.get_outgoing_header('Some-header') == ['Some-value']
				return "ok"

		headers, result = sendreq(
			ultramini.SiteMap(Simple()), GET_TEMPLATE % ('/', ))

		self.assertEqual(result, 'ok')
		assert 'Some-header: Some-value'.lower() in headers.lower()


	def test_post_request(self):
		class Simple(ultramini.Resource):
			def post(self, req, form):
				return '/'

		headers, result = sendreq(
			ultramini.SiteMap(Simple()), POST_TEMPLATE % ('/', 0, ''))

		assert headers.startswith('HTTP/1.1 303')


	def test_cached_resource(self):
		class Simple(ultramini.Resource):
			template = 0
			def willHandle(self, request):
				self.template = self.template + 1
				return self

		sitemap = ultramini.SiteMap(Simple())

		headers, result = sendreq(sitemap, GET_TEMPLATE % ('/', ))
		assert result == '1', ('Fail', result)

		headers, result = sendreq(sitemap, GET_TEMPLATE % ('/', ))
		assert result == '2', ('Fail', result)


	def test_uncached_resource(self):
		class Simple(ultramini.Resource):
			cache = False
			template = 0
			def willHandle(self, request):
				self.template = self.template + 1
				return self

		sitemap = ultramini.SiteMap(Simple())

		headers, result = sendreq(sitemap, GET_TEMPLATE % ('/', ))
		assert result == '1', (result, headers)

		headers, result = sendreq(sitemap, GET_TEMPLATE % ('/', ))
		assert result == '2', (result, headers)


## TODO Write cache tests
	
if __name__ == '__main__':
	unittest.main()
