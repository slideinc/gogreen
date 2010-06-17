'''
This doesn't totally work yet.

It's an exploration into hosting WSGI apps on the corohttpd mini-framework

ATM wsgi.input is basically totally broken -- cgi.FieldStorage initially looked
like it might do it right, but now the thing just hangs on post data. Again,
doesn't totally work yet.

--travis
'''

import sys
import coro
import corohttpd
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


class WSGIInput(object):
    def __init__(self, request):
        self._request = request
        self._infile = None

    def _start_reading(self):
        if not self._infile:
            fs = self._request.get_field_storage()
            fs.read_binary()
            self._infile = fs.file
            self._infile.seek(0)

    def read(self, size):
        self._start_reading()
        return self._infile.read(size)

    def readline(self):
        self._start_reading()
        return self._infile.readline()

    def readlines(self, hint=None):
        self._start_reading()
        return self._infile.readlines()


class WSGIAppHandler(object):
    def __init__(self, app):
        self.app = app

    def match(self, request):
        return True

    def handle_request(self, request):
        address = request.server().socket.getsockname()

        environ = {
            'wsgi.version': (1, 0),
            'wsgi.url_scheme': 'http',
            'wsgi.input': WSGIInput(request),
            'wsgi.errors': sys.stderr, #XXX: fix this
            'wsgi.multithread': False,
            'wsgi.multiprocess': False,
            'wsgi.run_once': False,
            'SCRIPT_NAME': '',
            'PATH_INFO': request._path,
            'SERVER_NAME': address[0],
            'SERVER_PORT': address[1],
            'REQUEST_METHOD': request.method(),
            'SERVER_PROTOCOL': request._connection.protocol_version,
        }

        if request._query:
            environ['QUERY_STRING'] = request._query

        clheader = request.get_header('Content-Length')
        if clheader:
            environ['CONTENT_LENGTH'] = clheader

        ctheader = request.get_header('Content-Type')
        if ctheader:
            environ['CONTENT_TYPE'] = ctheader

        for name, value in request.get_headers().items():
            environ['HTTP_%s' % name.replace('-', '_').upper()] = value

        headers_sent = [False]

        def start_response(status, headers, exc_info=None):
            if exc_info and collector[1]:
                raise exc_info[0], exc_info[1], exc_info[2]
            else:
                exc_info = None

            # this is goofy -- get the largest status prefix that is an integer
            for index, character in enumerate(status):
                if index and not status[:index].isdigit():
                    break

            code = int(status[:index - 1])
            request.response(200)

            for name, value in headers:
                request.set_header(name, value)

            headers_sent[0] = True

            return request.push

        body_iterable = self.app(environ, start_response)
        for chunk in body_iterable:
            request.push(chunk)


def serve(address, wsgiapp):
    server = corohttpd.HttpServer(args=(address,))
    server.push_handler(WSGIAppHandler(wsgiapp))
    server.start()
    coro.event_loop(30.0)


def main():
    def hello_wsgi_world(environ, start_response):
        start_response('200 OK', [
            ('Content-Type', 'text/plain'),
            ('Content-Length', '13')])
        return ["Hello, World!"]

    serve(("", 8000), hello_wsgi_world)


if __name__ == '__main__':
    main()
