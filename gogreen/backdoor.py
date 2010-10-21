#!/usr/bin/env python
# -*- Mode: Python; tab-width: 4 -*-

# Copyright (c) 1999, 2000 by eGroups, Inc.
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


import code
import socket
import sys
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from gogreen import coro


PS1 = getattr(sys, "ps1", ">>> ")
PS2 = getattr(sys, "ps2", "... ")


class BackDoorClient(coro.Thread):
    def init(self):
        self.exit = False

    def run(self, clientsock, client_address):
        env = sys.modules['__main__'].__dict__.copy()
        console = code.InteractiveConsole(env)
        clientfile = clientsock.makefile('r')
        multiline_statement = []
        stdout, stderr = StringIO(), StringIO()

        clientsock.sendall(
                'Python ' + sys.version + '\r\n' + sys.copyright + '\r\n' + PS1)

        for input_line in clientsock.makefile('r'):
            if self.exit:
                break

            input_line = input_line.rstrip()
            source = '\n'.join(multiline_statement + [input_line])
            response = []

            stdout.seek(0)
            stderr.seek(0)
            stdout.truncate()
            stderr.truncate()
            real_stdout = sys.stdout
            real_stderr = sys.stderr
            sys.stdout = stdout
            sys.stderr = stderr
            try:
                result = console.runsource(source)
            finally:
                sys.stdout = real_stdout
                sys.stderr = real_stderr

            response.append(stdout.getvalue())
            response.append(stderr.getvalue())

            if response[-1] or not result:
                multiline_statement = []
                response.append(PS1)
            else:
                multiline_statement.append(input_line)
                response.append(PS2)

            clientsock.sendall(''.join(response))

        self.info("Backdoor connection closing")
        clientsock.close()


class BackDoorServer(coro.Thread):
    def init(self):
        self._exit = False
        self._s    = None

    def run(self, port=8023, ip=''):
        self._s = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
        self._s.set_reuse_addr()
        self._s.bind((ip, port))
        self._s.listen(1024)

        port = self._s.getsockname()[1]
        self.info('Backdoor listening on port %d' % (port,))

        while not self._exit:
            try:
                conn, addr = self._s.accept()
            except coro.CoroutineSocketWake:
                continue

            client = BackDoorClient(args = (conn, addr))
            client.start()

        self.info('Backdoor exiting (children: %d)' % self.child_count())

        self._s.close()
        self._s = None

        for child in self.child_list():
            child.shutdown()

        self.child_wait()
        return None

    def shutdown(self):
        if self._exit:
            return None

        self._exit = True

        if self._s is not None:
            self._s.wake()
#
# extremely minimal test server
#
if __name__ == '__main__':
    server = BackDoorServer()
    server.start()
    coro.event_loop (30.0)
#
# end...
