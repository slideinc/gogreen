#!/usr/bin/python
#
'''heartbeat

a common pattern is to do some work every N seconds. this example demestrates
how to do that with gogreen.
'''
import signal
import sys

from gogreen import coro
from gogreen import emulate
from gogreen import start
from gogreen import backdoor

# do monkeypatching of various things like sockets, etc.
#
emulate.init()



class Heartbeat(coro.Thread):

    def __init__(self, period, *args, **kwargs):
        super(Heartbeat, self).__init__(*args, **kwargs)
        self._period = period
        self._cond   = coro.coroutine_cond()
        self._exit   = False

    def run(self):
        self.info('Heartbeat: start')
        while not self._exit:
            self.info('bah-bump')
            self._cond.wait(self._period)
        self.info('Heartbeat: end')

    def shutdown(self):
        self._exit = True
        self._cond.wake_all()
        
def run(here, log, loglevel, logdir, **kwargs):
    heart = Heartbeat(
        1.2, # every 1.2 seconds
        log = log,
        )
    heart.set_log_level(loglevel)
    heart.start()

    if 'backport' in here:
        back = backdoor.BackDoorServer(
            args = (here['backport'],),
            log  = log, 
            )
        back.set_log_level(loglevel)
        back.start()

    def shutdown_handler(signum, frame):
        heart.shutdown()
        back.shutdown()

    signal.signal(signal.SIGUSR2, shutdown_handler)

    try:
        coro.event_loop()
    except KeyboardInterrupt:
        pass

    return None

if __name__ == '__main__':
    conf = {
        0 : {'lockport' : 6581, 'backport' : 5500, }, 
    }
    value = start.main(
        conf,
        run,
        name = 'heartbeat',
        )
    sys.exit(value) 
