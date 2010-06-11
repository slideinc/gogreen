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

"""Ultra mini lightweight framework
"""

import os, re, types, urllib, cgi, cStringIO, sys, tempfile, Cookie
import exceptions

try:
    import amf
except:
    amf = None
try:
    import amfast
except:
    amfast = None

class LocationDeprecationWarning(DeprecationWarning):
	pass


class xml(object):
	"""An object which contains only xml bytes.
	"""
	def __init__(self, string):
		self.string = string


####################
####################
## Argument Converters
####################
####################


def arguments(*args):
	"""A Decorator which you wrap around a function or a method to indicate
	which magical objects you want passed to your functions which are
	embedded in a Resource's template.

	The magical values you can pass are:
		LOCATION
		REQUEST
		SITE
		RESOURCE
		METHOD
		URI

	For example, to pass a function the SiteMap instance, do this:

		@arguments(SITE)
		def foo(site):
			print "The site!"

	The values which you pass a string argument to get variables out of the
	request environment (which also take a default) are:

		QUERY
		QUERIES
		ARG
		ARGS
		COOKIE
		HEADER

	For example, to pass a query parameter to a function, with a default
	if the parameter is not present, do this:

		@arguments(QUERY('foo', 'default'))
		def foo(foo):
			print foo

	All the magic objects which take string values have a __getattr__
	implementation for syntatic convenience when you do not need to
	pass a default value (None will be used as the default value):

		@arguments(QUERY.foo)
		def foo(foo):
			print foo
	"""
	def decorate(func):
		func.arguments = args
		return func
	return decorate


class LOCATION(object):
	"""The Location object representing the URL currently being rendered.
	"""


class REQUEST(object):
	"""The Request object representing the current web request.
	"""


class SITE(object):
	"""The SiteMap object in which the resource which is currently being
	rendered resides.
	"""


class RESOURCE(object):
	"""The currently-being-rendered resource.
	"""


class METHOD(object):
	"""The method of the current request.
	"""


class URI(object):
	"""The URI of the current request.
	"""


class Thing(object):
	"""A lazy thing. Lazy things have a name and a default
	value, and are looked up lazily from the request environment.
	"""
	def __init__(self, name, default=None):
		self.name = name
		self.default = default

	def __getattr__(self, name):
		return type(self)(name)

	def __call__(self, name, default=None):
		return type(self)(name, default)


class QUERY(Thing):
	"""A query argument (url parameter)
	"""


class QUERIES(Thing):
	"""Produce a generator of all query arguments with the given name
	"""


class ARG(Thing):
	"""A form post variable.
	"""


class ARGS(Thing):
	"""All form post variables with the given name.
	"""


class COOKIE(Thing):
	"""A cookie with the given name.
	"""


class HEADER(Thing):
	"""A header with the given name.
	"""


def getQueries(request, arg):
	queries = tuple(request.get_query(arg.name))
	if not queries:
		return arg.default
	return queries


def getQuery(request, arg):
	queries = getQueries(request, arg)
	if queries == arg.default:
		return arg.default
	return queries[0]


def getArgs(request, arg):
	val = request.get_arg_list(arg.name)
	if not val:
		return arg.default
	return val


def getCookie(request, arg):
	"""get a cookie woot.
	"""
	if not hasattr(request, '_simple_cookie'):
		cookie = request.get_header('Cookie')
		if not cookie:
			return arg.default
		c = Cookie.SimpleCookie()
		c.load(cookie)
		request._simple_cookie = c
	cook_value = request._simple_cookie.get(arg.name)
	if cook_value and cook_value.value:
		return cook_value.value
	return arg.default


def getLocation(request, arg):
	import warnings
	if True:
		warnings.warn(
			"The location is deprecated. Use the request instead.",
			LocationDeprecationWarning, 2)
	return request


## Argument converters are passed to the "arguments" decorator.
## Immediately before the decorated function is called, they are invoked.
## They are passed information about the current request
## and they should return the lazy value they represent.
argumentConverters = {
	LOCATION: getLocation,
	REQUEST: lambda request, arg: request,
	SITE: lambda request, arg: request.site,
	RESOURCE: lambda request, arg: request.resource,
	METHOD: lambda request, arg: request.method(),
	URI: lambda request, arg: request.uri(),
	QUERY: getQuery,
	QUERIES: getQueries,
	ARG: lambda request, arg: request.get_arg(arg.name, arg.default),
	ARGS: getArgs,
	COOKIE: getCookie,
	HEADER: lambda request, arg: request.get_header(arg.name, arg.default),
}


## These things are all prototype objects; they have __getattr__
## and __call__ factories which are used for generating clones
## of them. The lines below make the names that people use
## into instances instead of classes.
LOCATION = LOCATION()
REQUEST = REQUEST()
SITE = SITE()
RESOURCE = RESOURCE()
METHOD = METHOD()
URI = URI()
QUERY = QUERY('')
QUERIES = QUERIES('')
ARG = ARG('')
ARGS = ARGS('')
COOKIE = COOKIE('')
HEADER = HEADER('')


####################
####################
## End Argument Converters
####################
####################


####################
####################
## Rendering Converters
####################
####################


## Rendering converters
## When objects are placed into a DOM which is rendered to be
## sent to the client as HTML, they are run through a series
## of rendering converters until nothing but a single "xml"
## instance is left.

def convertFunction(request, function):
	args = []
	for arg in getattr(function, 'arguments', ()):
		converter = argumentConverters.get(type(arg))
		if converter is None:
			args.append(arg)
		else:
			args.append(converter(request, arg))

	return function(*args)


convertSequence = lambda request, sequence: xml(
	''.join([request.convert(request, x) for x in sequence]))


convertNumber = lambda request, number: xml(str(number))


class show(object):
	"""A marker which will lazily look up a show_* method
	from the currently-being-rendered resource when encountered
	in a template.
	"""
	def __init__(self, name, args=()):
		self.name = name
		self.args = args

	def __getattr__(self, name):
		return type(self)(name)

	def __call__(self, *args):
		return type(self)(self.name, self.args + args)

	def __iter__(self):
		return iter(())


def convertShow(request, theDirective):
	@arguments(RESOURCE, *theDirective.args)
	def convert(resource, *args):
		return getattr(resource, "show_%s" % (theDirective.name, ))(*args)
	return convert


class stan(object):
	def __init__(
		self, tag, attributes, children, pattern=None, show=None, clone=False):
		(self.tag, self.attributes, self.children, self.pattern, self.show, 
		 self.clone) = (
			tag, attributes, children, pattern, show, clone)

	def __getitem__(self, args):
		"""Add child nodes to this tag.
		"""
		if not isinstance(args, (tuple, list)):
			args = (args, )
		if not isinstance(args, list):
			args = list(args)
		if self.clone:
			return type(self)(
				self.tag, self.attributes.copy(), [
					x for x in self.children] + args,
				self.pattern, self.show, False)
		self.children.extend(args)
		return self

	def __call__(self, **kw):
		"""Set attributes of this tag. There are two special names
		which are reserved:

			pattern
				Make it possible to find this node later using the
				findPattern function

			show
				When this node is rendered, instead render the
				value passed as the "show" value.
		"""
		if kw.has_key('pattern'):
			pattern = kw.pop('pattern')
		else:
			pattern = self.pattern

		if kw.has_key('show'):
			show = kw.pop('show')
		else:
			show = self.show

		if self.clone:
			newattrs = self.attributes.copy()
			newattrs.update(kw)
			return type(self)(
				self.tag, newattrs, self.children[:], pattern, show, False)
		self.attributes.update(kw)
		self.pattern = pattern
		self.show = show
		return self

	def cloneNode(self):
		return type(self)(
			self.tag, self.attributes.copy(), self.children[:], None, 
			self.show, False)


def tagFactory(tagName):
	return stan(tagName, {}, [], None, None, True)


def findPattern(someStan, targetPattern):
	"""Find a node marked with the given pattern, "targetPattern",
	in a DOM object, "someStan"
	"""
	pat = getattr(someStan, 'pattern', None)
	if pat == targetPattern:
		return someStan.cloneNode()
	for child in getattr(someStan, 'children', []):
		result = findPattern(child, targetPattern)
		if result is not None:
			return result.cloneNode()


## TODO: Inline elements shouldn't have any whitespace after them or before
## the closing tag.


def convertStan(request, theStan):
	## XXX this probably isn't necessary
	request.tag = theStan
	if theStan.show is not None:
		return theStan.show
	if theStan.pattern is not None:
		return xml('')

	attrs = ''
	if theStan.attributes:
		for key, value in theStan.attributes.items():
			attrs += ' %s="%s"' % (
				key, request.convert(request, value).replace('"', '&quot;'))
	#"
	depth = getattr(request, 'depth', 0)
	indent = '  ' * depth
	request.depth = depth + 1
	if theStan.tag in inline_elements:
		template = """<%(tag)s%(attrs)s>%(children)s</%(tag)s>"""
	else:
		template = """
%(indent)s<%(tag)s%(attrs)s>
%(indent)s  %(children)s
%(indent)s</%(tag)s>
"""
	result = template % dict(
		indent=indent, tag=theStan.tag, attrs=attrs,
		children=request.convert(request, theStan.children).strip())

	request.depth -= 1
	return xml(result)


inline_elements = [
	'a', 'abbr', 'acronym', 'b', 'basefont', 'bdo', 'big', 'br', 'cite',
	'code', 'dfn', 'em', 'font', 'i', 'img', 'input', 'kbd', 'label',
	'q', 's', 'samp', 'select', 'small', 'span', 'strike', 'strong',
	'sub', 'sup', 'textarea', 'tt', 'u', 'var']
inline_elements = dict((x, True) for x in inline_elements)


tags_to_create = [
'a','abbr','acronym','address','applet','area','b','base','basefont','bdo',
'big','blockquote','body','br','button','caption','center','cite','code',
'col','colgroup','dd','dfn','div','dl','dt','em','fieldset','font','form',
'frame','frameset','h1','h2','h3','h4','h5','h6','head','hr','html','i',
'iframe','img','input','ins','isindex','kbd','label','legend','li','link',
'menu','meta','noframes','noscript','ol','optgroup','option','p','param',
'pre','q','s','samp','script','select','small','span','strike','strong',
'style','sub','sup','table','tbody','td','textarea','tfoot','th','thead',
'title','tr','tt','u','ul','var'
]


class tags(object):
	"""A namespace for tags, so one can say "from ultramini import tags"
	and have access to all tags as tags.html, tags.foo, etc
	"""
	pass


for tag in tags_to_create:
	T = tagFactory(tag)
	globals()[tag] = T
	setattr(tags, tag, T)
del tag
del T
del tags_to_create


converters = {
	unicode: lambda request, uni: uni.encode('utf8'),
	list: convertSequence,
	tuple: convertSequence,
	types.GeneratorType: convertSequence,
	types.FunctionType: convertFunction,
	types.MethodType: convertFunction,
	types.UnboundMethodType: convertFunction,
	int: convertNumber,
	float: convertNumber,
	long: convertNumber,
	show: convertShow,
	stan: convertStan,
	str: lambda request, string: xml(
		string.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
}
converters.update(argumentConverters)


show = show('')


def registerConverter(convertType, converter):
	"""Call this to register a converter for one of your custom types.
	"""
	converters[convertType] = converter


def convert(request, stuff):
	"""Recursively apply stuff converters until we get an xml instance.
	"""
	if not isinstance(stuff, xml):
		print 'DEPRECATING auto-escaping. URI: %s RESOURCE: %r' % (
			request.uri(),
			request.resource)

	while not isinstance(stuff, xml):
		convert = converters.get(type(stuff))
		if convert is None:
			raise RuntimeError, "Converter for type %r (%r) not found." % (
				type(stuff), stuff)
		stuff = convert(request, stuff)
	return stuff.string


####################
####################
## End Rendering Converters
####################
####################



####################
####################
## Incoming Type Converters
####################
####################


## When arguments come in (as GET url parameters or POST arguments)
## they can be converted from strings to Python types using incoming
## type converters. To use them, wrap a get_* or post_* method
## using the annotate decorator. When a get_ or post_ method
## is called (by having a corresponding ?get= parameter or action
## argument) the arguments named in the decorator will be converted
## before they are passed to the method.


def annotate(*args, **kwargs):
	'''annotate

	  A decorator which annotates a handler function (GET and/or POST
	method) with pre and/or post request filter functions as well as
	mapping functions for each argument the action method will receive.

	First argument: return converter(s)

	    The return value converter, if present, can be either a single
	  callable or a list of callables. The return value of the annotated
	  function is passed as the only argument to the callable and the
	  callable returns a string which is sent as the body of the HTTP
	  response. When the return converter is a list of callables the call
	  argument and return value of each are chained together. Only the
	  last callable needs to return a string for the body of the HTTP
	  response. If the first argument is not present then the return
	  value of the annotated function must be a string which will be the
	  body of the HTTP response.


	Second argument: input converter(s)

	    The second argument, if present, is the input converter, and can
	  be either a single callable or a list of callables. Each is called
	  with three arguments; 1) the handler instance to which the annotated
	  method belongs, 2) the request object, 3) the request field storage,
	  in the case of a POST, or the request query parameters, in the case
	  of a GET, represented as a dictionary. The return value can either be
	  None, or a new dictionary representation of the third argument which
	  will be chained into the next input converter callable and/or used
	  for the annotated mapping functions described in the next section.

	Keyword arguments: action mapping functions

	    Each keyword argument is either a callable or one of the well
	  known constants below. In both cases each keyword represents the
	  method parameter of the same name.

	    In the case that the keyword argument is set to one of the well
	  known constants the object/data represented by the constants, as
	  described below, will be passed into the method as the parameter
	  of the same name as the keyword argument.

	    In the case that the keyword argument is a callable AND there is
	  a query parameter, in the case of a GET, or a field argument, in
	  the case of a POST, of the same name, then the value of the request
	  parameter/argument is passed into the callable. The result of the
	  callable is then passed into the method as the parameter
	  of the same name as the keyword argument.

	Keyword argument constants:

	  (used as annotation types for retrieving specific information about
	   the request)
		
	  REQUEST - request (corohttpd.HttpRequest)
	  COOKIE  - cookies (Cookie.SimpleCookie)    request.get_cookie()
	  HEADER  - HTTP header  (mimetools.Message) request.get_headers()
	  METHOD  - HTTP method (str)                request.method()
	  URI     - HTTP uri (str)                   request.uri()
	  QUERIES - query parameters (list)          request.get_query_pairs()
	  ARGS    - arguments (cgi.FieldStorage)     request.get_field_storage()
	    
	
	Examples:

	To expose a method to be called by placing ?action=foo
	in the query string with a single parameter bar of type int:
	(e.g. /handler?action=foo&bar=2)

	@method()
	@annotate(bar = int)
	def action_foo(self, bar):
		print bar

	To express lists of things:

	@method()
	@annotate(bar=[int])
	def action_foo(self, bar):
		for someInt in bar:
			print someInt

	Include converters; return converter to ensure a string response, a
	input converter to decode the crypto on bar:
	

	def fini(value):
		return str(value)

	def init(rec, req, param):
		if param.has_key('bar'):
			param['bar'] = decode(param['bar'])
		return param

	@annotate(fini, init, bar = int)
	def get_default(self, bar = 0):
		print bar
		return 2 * bar

	Include request object to set response header:

	@annotate(fini, init, req = REQUEST, bar = int)
	def get_default(self, req, bar = 0):
		print bar
		req.set_header('X-Bar', bar) 
		return 2 * bar
	'''
	TODO = '''

	Dicts of things should be expressed by presenting
	arguments with a common prefix separated by a dot. For
	example:

	?get=foo&someDict.bar=1&someDict.baz=foo

	Would call:

	@annotate(someDict={bar: int, baz: str})
	def get_foo(self, someDict):
		print someDict['bar']
		print someDict['baz']

	This is still TODO. (Perhaps colon instead of dot?)
	'''

	returnConverter = args and args[0]
	inputConverter = None
	if len(args) == 2:
		inputConverter = args[1]
	def decorate(method):
		method.returnConverter = returnConverter
		method.inputConverter = inputConverter
		method.types = kwargs
		return method
	return decorate


def convertList(request, theList):
	request.target = request.target[0]
	if not isinstance(theList, list):
		theList = [theList]
	return [convertTypes(request, item) for item in theList]


def convertTuple(request, theTuple):
	stuff = []
	for i, subtype in enumerate(request.target):
		request.target = subtype
		stuff.append(convertTypes(request, theTuple[i]))
	return tuple(stuff)


def convertDict(request, nothing):
	theDict = {}
	for field in request.get_field_storage().list:
		if field.name.startswith(request.target_name + '.'):
			theDict[field.name.split('.', 1)[-1]] = field.value
	target = request.target
	result = {}
	for (key, value) in target.items():
		request.target = value
		result[key] = convertTypes(request, theDict[key])
	return result


typeConverters = converters.copy()
typeConverters.update({
	int: lambda target, value: int(value),
	float: lambda target, value: float(value),
	list: convertList,
	tuple: convertTuple,
	dict: convertDict,
	str: lambda target, value: value,
	unicode: lambda target, value: value.decode('utf8'),
	types.FunctionType: lambda request, value: request.target(request, value),
	object: lambda target, value: value,
	bool: lambda target, value: bool(value),

	type(REQUEST): lambda request, value: request,
	type(SITE): lambda request, value: request.site,
	type(RESOURCE): lambda request, value: request.resource,
	type(METHOD): lambda request, value: request.method(),
	type(URI): lambda request, value: request.uri(),
	type(QUERIES): lambda request, value: request.get_query_pairs(),
	type(ARGS): lambda request, value: request.get_field_storage(),
	type(COOKIE): lambda request, value: request.get_cookie(),
	type(HEADER): lambda request, value: request.get_headers(),
})


def registerTypeConverter(convertType, converter):
	typeConverters[convertType] = converter


def convertTypes(request, value):
	target = request.target
	converter = typeConverters.get(type(target))
	if converter is None:
		converter = typeConverters.get(target)
	if converter is None:
		raise ValueError, "No converter for type %s (%s)" % (target, value)
	return converter(request, value)


def storage_to_dict(fs):
	rc = {}
	for k, v in dict(fs).iteritems():
		if isinstance(v, list):
			rc[k] = [isinstance(e, cgi.MiniFieldStorage) and e.value or e for e in v]
		elif isinstance(v, basestring):
			rc[k] = v
		elif not hasattr(v, 'value'):
			rc[k] = v		
		else:
			rc[k] = v.value
	return rc

def applyWithTypeConversion(callable_method, request, **kwargs):
	'''
		Currently useful kwargs:
			`resource` => instance of the resource executing
	'''
	types = getattr(callable_method, 'types', {})
	returnConverter = getattr(callable_method, 'returnConverter', None)
	inputConverter = getattr(callable_method, 'inputConverter', None)

	fs = request.get_field_storage()
	parameters = storage_to_dict(fs)

	if inputConverter:
		if not isinstance(inputConverter, (list, tuple)):
			inputConverter = (inputConverter, )

		for conv in inputConverter:
			rc = conv(kwargs.get('resource'), request, parameters)
			parameters = rc or parameters

	try:
		converted = {}
		for (key, value) in types.items():
			request.target_name = key
			request.target = value
			if parameters.has_key(key):
				val = parameters[key]
			else:
				val = getattr(value, 'default', None)

			try:
				val = convertTypes(request, val)
			except (TypeError, ValueError):
				val = None

			if val is not None:
				converted[key] = val

		result = callable_method(**converted)
	except TypeError:
		raise
	if result is None:
		result = ''
	if not returnConverter:
		return False, result

	if not isinstance(returnConverter, (list, tuple)):
		returnConverter = (returnConverter, )

	for conv in returnConverter:
		result = conv(result)

	return True, result


##AMF Converter Support
class AMFDecodeException(exceptions.Exception):
	pass

class AMFFieldStorage(cgi.FieldStorage):
	def __init__(self, *args, **kwargs):
		self.classmapper = kwargs.pop('class_mapper', None)
		cgi.FieldStorage.__init__(self, *args, **kwargs)

	def read_single(self):
		qs = self.fp.read(self.length)
		if qs.strip() == '':
			raise AMFDecodeException('empty AMF data on decode')
		ct = amfast.context.DecoderContext(qs, amf3=True,
				class_def_mapper=self.classmapper)
		data = amfast.decoder.decode(ct)
		ct = None
		
		self.list = [cgi.MiniFieldStorage(k,v) for k,v in data.amf_payload.iteritems()]	
		self.skip_lines()	
		
####################
####################
## End Incoming Converters
####################
####################



####################
####################
## URL Traversal
####################
####################


## SiteMap has a root Resource and traverses the URL using the Resource
## interface.


class SiteMap(object):
	"""An object which is capable of matching Resource objects to URLs and
	delegating rendering responsibility to the matched object.

	This class implements the corohttpd handler interface: match
	and handle_request. It adapts this interface to something more similar
	to the Nevow URL traversal interface: findChild and handle (like
	locateChild and renderHTTP in Nevow).
	"""
	def __init__(self, root, **parameters):
		self.root   = root
		root.child_ = root

		self.__dict__.update(parameters)

	def __del__(self):
		self.root._clear_child_()

	def match(self, request):
		request.site = self

		remaining = tuple(request._path.split('/')[1:])
		if request._path.find('://') < 0:
			# Provided our path doesn't contain another URL, trim redundant slashes
			parts = request._path.split('/')[1:]
			remaining = tuple((s for i, s in enumerate(parts) if s or i == (len(parts) - 1)))

		child     = self.root
		handled   = ()

		while child is not None and remaining:
			child, handled, remaining = child.findChild(request, remaining)

		if child is None:
			return False
		#
		# Allow resources to delegate to someone else,
		# even if they have been selected as the target of rendering
		#
		newChild = child.willHandle(request)
		if newChild is None:
			return False

		if newChild is not child:
			child = newChild

		request.segments = handled
		request.resource = child

		return True

	def handle_request(self, request):
		request.convert = convert
		resource = request.resource
		request.set_name(resource)
		resource.handle(request)

		request.resource = None
#
# Resource specific decorator(s).
#
def method_post(*args, **kwargs):
	def decorate(m):
		m.ultramini_method = ['post']
		m.ultramini_inheritable = kwargs.get('inherit')
		return m
	if args:
		return decorate(args[0])
	return decorate

def method_get(*args, **kwargs):
	def decorate(m):
		m.ultramini_method = ['get']
		m.ultramini_inheritable = kwargs.get('inherit')
		return m
	if args:
		return decorate(args[0])
	return decorate

def method(*args, **kwargs):
	def decorate(m):
		m.ultramini_method = ['get', 'post']
		m.ultramini_inheritable = kwargs.get('inherit')
		return m
	if args:
		return decorate(args[0])
	return decorate


class Resource(object):
	contentType = 'text/html'
	template = "Resources must provide a template attribute."
	_amf_class_mapper = None
	
	global_public_methods = {}

	def __init__(self, *args, **kwargs):
		self.__init_global__()

		self._public_methods = self.global_public_methods[hash(self.__class__)]
		return super(Resource, self).__init__(*args, **kwargs)

	def __init_global__(self):
		if self.global_public_methods.has_key(hash(self.__class__)):
			return None

		data = {}
		for name in dir(self):
			element = getattr(self, name)
			for label in getattr(element, 'ultramini_method', []):
				if self.__class__.__dict__.get(name):
					data.setdefault(label, []).extend([name])
				elif getattr(element, 'ultramini_inheritable', False):
					data.setdefault(label, []).extend([name])

		self.global_public_methods[hash(self.__class__)] = data

	def __call__(self, request, name):
		return self

	_child_re = re.compile(r'^child_')
	def _clear_child_(self):
		"""_clear_child_ deletes this object's 'child_' attribute,
		then loops through all this object's attributes, and
		recursively calls the _clear_child method of any child_*
		attributes 

		Arguments:
		self: mandatory python self arg
		"""
		try:
			del self.child_
		except AttributeError, e:
			pass

		for attr in filter(self._child_re.match, dir(self)):
			try:
				getattr(self, attr)._clear_child_()
			except AttributeError:
				pass

	def willHandle(self, request):
		"""This Resource is about to handle a request.
		If it wants to delegate to another Resource, it can return it here.
		"""
		return self

	def findChild(self, request, segments):
		"""External URL segment traversal API. This method MUST always
		return a tuple of:

			(child, handled, remaining)

		child may be None to indicate a 404. Handled should be a tuple
		of URL segments which were handled by this call to findChild;
		remaining should be a tuple of URL segments which were not
		handled by this call to findChild.

		findChild can be overriden to implement fancy URL traversal
		mechanisms such as handling multiple segments in one call,
		doing an internal server-side redirect to another resource and
		passing it segments to handle, or delegating this segment
		to another resource entirely. However, for most common use
		cases, you will not override findChild.

		Any methods or attributes named child_* will be mapped to
		URL segments. For example, if an instance of Root is set
		as the root object, the urls "/foo" and "/bar" will be valid:

		class Root(Resource):
			child_foo = Resource()

			def child_bar(self, request, bar):
				return Resource()

		Finally, if a childFactory method is defined it will be called
		with a single URL segment:

		class Root(Resource):
			def childFactory(self, request, childName):
				## For URL "/foo" childName will be "foo"
				return Resource()
		"""
		current, remaining = segments[0], segments[1:]

		childFactory = getattr(
			self,
			'child_%s' % (current, ),
			self.childFactory)

		return childFactory(request, current), (current, ), remaining

	def childFactory(self, request, childName):
		"""Override this to produce instances of Resource to represent the next
		url segment below self. The next segment is provided in childName.
		"""
		return None

	def handle(self, request):
		request.set_header('Content-Type', self.contentType)
		handler = getattr(
			self, "handle_%s" % (request.method(), ), self.handle_default)
		handler(request)

	def handle_default(self, request):
		request.push(
			"<html><body>No handler for method %r present.</body></html>" %
			(request.method(), ))

	def handle_get(self, request):
		get_method = self.getAction('get', request)

		if get_method is None:
			request.write(self.template)
		else:
			converted, result = applyWithTypeConversion(get_method, request,
				resource=self)
			request.write(result)

	################
	## AMF Specific
	################
	
	@classmethod
	def mapped_classes(cls):
		return [
			amf.AMFRequest,
		]

	def amf_class_mapper(self):
		'''
			Return an amfast ClassDefMapper object to properly deserialize objects
			in this request.

			Note: this function may be called multiple times
		'''
		if not self._amf_class_mapper:
			self._amf_class_mapper = amfast.class_def.ClassDefMapper()
			# Standard required mappings
			amf.registerClassToMapper(amf.AMFRequest, self._amf_class_mapper)
			amf.registerClassToMapper(amf.AMFResponse, self._amf_class_mapper)
			amf.registerClassToMapper(amf.GenericAMFException, self._amf_class_mapper)
			amf.registerClassToMapper(amf.AMFError, self._amf_class_mapper)

			# Map classes for subclasses
			for cls in self.mapped_classes():
				amf.registerClassToMapper(cls, self._amf_class_mapper)
		return self._amf_class_mapper

	################
	## END AMF Specific
	################
	
	def getFieldStorage(self, data, headers, environ):
		'''getFieldStorage

		Result is an instance of cgi.FieldStorage or subclass. This is
		provided as a hook for subclasses to override if they want to
		parse the incoming POST data differently than a basic FieldStorage.
		For example, imgsrv/server overrides this to trick FieldStorage
		into saving binary parts directly into the image file directory,
		to avoid having to copy the file from a temporary file into the
		final location.

		data    - readable file-stream
		headers - request header dicttionary
		environ - request environment dictionary
		'''
		if headers.getheader('Content-Type') == 'application/x-amf' \
                and amf and amfast:
			return AMFFieldStorage(data, headers, environ=environ, 
				class_mapper=self.amf_class_mapper())
		return cgi.FieldStorage(
			data,
			headers,
			environ = environ,
			keep_blank_values = 1)

	def getAction(self, key, request):
		fs = request.get_field_storage()
		actions = []
		methods = getattr(self, '_public_methods', {})

		for name in methods.get(key, []):
			parts = name.split('_')
			data = '_'.join(parts[1:])

			for value in fs.getlist(parts[0]):
				if value != data:
					continue

				method = getattr(self, name, None)
				if method is None:
					continue

				actions.append(method)

		if not actions:
			return getattr(
				self,
				'%s_%s' % (key, fs.getfirst(key, 'default')),
				None)

		method = actions.pop()
		if actions:
			raise RuntimeError('multiple methods requested', method, actions)

		return method

	def handle_post(self, request):
		fs = request.get_field_storage()
		post_method = self.getAction('post', request)

		if post_method is None:
			result = self.post(request, fs)
			converted = False
		else:
			converted, result = applyWithTypeConversion(post_method, request, 
				resource=self)

		if not result:
			request.write(self.template)
			return None

		if converted:
			request.write(result)
			return None

		request.set_header('Location', request.convert(request, result))
		request.response(303)
		request.write('')


	def post(self, request, form):
		"""post

		Override this to handle a form post.
		Return a URL to be redirected to.

		request: An HttpRequest instance representing the place the form
		          was posted to.
		form: A cgi.FieldStorage instance representing the posted form.
		"""
		return request.uri()


class MovedPermanently(Resource):
	def __init__(self, location):
		self.location = location

	@annotate(req=REQUEST)
	def get_default(self, req):
		req.set_header(
			'Location',
			self.location)
		req.response(301) # Moved Permanently
		return self.location


class MovedTemporarily(Resource):
	def __init__(self, location):
		self.location = location

	@annotate(req=REQUEST)
	def get_default(self, req):
		req.set_header(
			'Location',
			self.location)
		req.response(302) # Moved Temporarily
		return self.location

	
class SeeOther(Resource):
	def __init__(self, location):
		self.location = location

	@annotate(req=REQUEST)
	def get_default(self, req):
		req.set_header(
			'Location',
			self.location)
		req.response(303) # See Other
		return self.location


class DebugLoggingResource(Resource):
	def enable_debug_logging(self, req, **kwargs):
		if kwargs.has_key('loglevel'):
			req.log_level(kwargs['loglevel'])

		if kwargs.get('httpread', False):
			req.connection().set_debug_read()
			req.connection().add_debug_read_data(req.requestline)
			req.connection().add_debug_read_data('')

			for header in req.get_headers().items():
				req.connection().add_debug_read_data('%s: %s' % header)

		return None

	def findChild(self, req, segments):
		debug = {}

		for value in req.get_query('ultradebug'):
			try:
				val = value.split('_')
				key = '_'.join(val[:-1])
				val = val[-1]

				key, val = key and (key, val) or (val, True)

				try:
					debug[key] = int(val)
				except (TypeError, ValueError):
					debug[key] = val
			except:
				print 'Cannot parse debug identifier <%s>' % value

		self.enable_debug_logging(req, **debug)
		return super(DebugLoggingResource, self).findChild(req, segments)
