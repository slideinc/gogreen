import logging.handlers

import basebot


class LogBot(basebot.Bot):
    filename_format = "%s.log"
    message_format = "[%(asctime)s] %(message)s"

    def __init__(self, *args, **kwargs):
        super(LogBot, self).__init__(*args, **kwargs)
        self.logs = {}

    def join(self, room, password=None):
        super(LogBot, self).join(room, password)

        handler = logging.handlers.RotatingFileHandler(
                self.filename_format % room, 'a', 6291456, 5)
        handler.setFormatter(logging.Formatter(self.message_format))
        logger = logging.Logger(room)
        logger.addHandler(handler)
        self.logs[room] = logger

        self.message(room, "(this conversation is being recorded)")

    def on_privmsg(self, cmd, args, prefix):
        sender = prefix.split("!", 1)[0]
        to, msg = args
        if to in self.logs:
            self.logs[to].info("<%s> %s" % (sender, msg))

        parent = super(LogBot, self)
        if hasattr(parent, "on_privmsg"):
            parent.on_privmsg(cmd, args, prefix)

    def on_join(self, cmd, args, prefix):
        sender = prefix.split("!", 1)[0]
        room = args[0]

        if room in self.logs:
            self.logs[room].info("<%s> joined %s" % (sender, room))

        parent = super(LogBot, self)
        if hasattr(parent, "on_join"):
            parent.on_join(cmd, args, prefix)

    def on_part(self, cmd, args, prefix):
        sender = prefix.split("!", 1)[0]
        room = args[0]

        if room in self.logs:
            self.logs[room].info("<%s> left %s" % (sender, room))

        parent = super(LogBot, self)
        if hasattr(parent, "on_part"):
            parent.on_part(cmd, args, prefix)
