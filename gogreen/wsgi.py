'''
untested, exploratory module serving a WSGI app on the corohttpd framework
'''

import coro
import corohttpd
import logging
import sys


class _WSGIInput(object):
    def __init__(self, request):
        self._request = request
        self._length = int(request.get_header('Content-Length', '0'))

    def read(self, size=-1):
        conn = self._request._connection
        gathered = len(conn.buffer)

        # reading Content-Length bytes should behave like EOF
        if size >= 0:
            size = min(size, self._length)

        if not size:
            return ''

        while 1:
            data = conn.connection.recv(corohttpd.READ_CHUNK_SIZE)
            gathered += len(data)
            conn.buffer += data
            if not data:
                data, conn.buffer = conn.buffer, ''
                self._length -= len(data)
                return data
            if size >= 0 and gathered >= size:
                break

        data, conn.buffer = conn.buffer[:size], conn.buffer[size:]
        self._length -= len(data)
        return data

    def readline(self):
        conn = self._request._connection
        while 1:
            index = conn.buffer.find("\r\n")
            if index >= 0 or len(conn.buffer) >= self._length:
                if index < 0:
                    index = len(conn.buffer)
                result, conn.buffer = conn.buffer[:index], conn.buffer[index:]
                result = result[:self._length]
                self._length -= len(result)
                return result

            data = conn.connection.recv(corohttpd.READ_CHUNK_SIZE)
            if not data:
                break
            conn.buffer += data

        result, conn.buffer = conn.buffer, ''
        self._length -= len(result)
        return result

    def readlines(self, hint=None):
        return list(self._readlines())

    def _readlines(self):
        line = self.readline()
        while line:
            yield line
            line = self.readline()


class _WSGIErrors(object):
    def __init__(self, logger):
        self._log = logger

    def flush(self):
        pass

    def write(self, msg):
        self._log.log(logging.ERROR, msg)

    def writelines(self, lines):
        map(self.write, lines)


class WSGIAppHandler(object):
    def __init__(self, app):
        self.app = app

    def match(self, request):
        return True

    def handle_request(self, request):
        address = request.server().socket.getsockname()
        if request._connection._log:
            errors = _WSGIErrors(request._connection._log)
        else:
            errors = sys.stderr

        environ = {
            'wsgi.version': (1, 0),
            'wsgi.url_scheme': 'http',
            'wsgi.input': _WSGIInput(request),
            'wsgi.errors': errors,
            'wsgi.multithread': False,
            'wsgi.multiprocess': False,
            'wsgi.run_once': False,
            'SCRIPT_NAME': '',
            'PATH_INFO': request._path,
            'SERVER_NAME': address[0],
            'SERVER_PORT': address[1],
            'REQUEST_METHOD': request.method().upper(),
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
            request.response(code)

            for name, value in headers:
                request.set_header(name, value)

            headers_sent[0] = True

            return request.push

        body_iterable = self.app(environ, start_response)
        for chunk in body_iterable:
            request.push(chunk)


def serve(address, wsgiapp, access_log='', error_log=None):
    kwargs = {}
    if error_log:
        handler = logging.handlers.RotatingFileHandler(
                filename or 'log',
                'a',
                corohttpd.ACCESS_LOG_SIZE_MAX,
                corohttpd.ACCESS_LOG_COUNT_MAX)
        handler.setFormatter(logging.Formatter('%(message)s'))
        kwargs['log'] = logging.Logger('error')
        kwargs['log'].addHandler(handler)

    server = corohttpd.HttpServer(args=(address, access_log), **kwargs)
    server.push_handler(WSGIAppHandler(wsgiapp))
    server.start()
    coro.event_loop()


def main():
    def hello_wsgi_world(environ, start_response):
        start_response('200 OK', [
            ('Content-Type', 'text/plain'),
            ('Content-Length', '13')])
        return ["Hello, World!"]

    serve(("", 8000), hello_wsgi_world, access_log='access.log')


if __name__ == '__main__':
    main()
